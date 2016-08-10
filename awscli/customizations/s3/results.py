# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import sys
import threading
import logging
from collections import namedtuple

from s3transfer.subscribers import BaseSubscriber

from awscli.compat import queue
from awscli.customizations.s3.utils import uni_print
from awscli.customizations.s3.utils import human_readable_size
from awscli.customizations.s3.utils import relative_path
from awscli.customizations.s3.utils import WarningResult
from awscli.customizations.s3.executor import ShutdownThreadRequest


LOGGER = logging.getLogger(__name__)

QueuedResult = namedtuple(
    'QueuedResult',
    ['transfer_type', 'src', 'dest', 'total_transfer_size']
)
ProgressResult = namedtuple(
    'ProgressResult',
    ['transfer_type', 'src', 'dest', 'bytes_transferred',
     'total_transfer_size']
)
SuccessResult = namedtuple(
    'SuccessResult', ['transfer_type', 'src', 'dest']
)
FailureResult = namedtuple(
    'FailureResult', ['transfer_type', 'src', 'dest', 'exception']
)
CommandResult = namedtuple(
    'CommandResult', ['num_tasks_failed', 'num_tasks_warned'])


class BaseResultSubscriber(BaseSubscriber):
    TRANSFER_TYPE = None

    def __init__(self, result_queue):
        """Subscriber to send result notifications during transfer process

        :param result_queue: The queue to place results to be processed later
            on.
        """
        self._result_queue = result_queue
        self._transfer_type = None

    def on_queued(self, future, **kwargs):
        src, dest = self._get_src_dest(future)
        queued_result = QueuedResult(
            self.TRANSFER_TYPE, src, dest, future.meta.size)
        self._result_queue.put(queued_result)

    def on_progress(self, future, bytes_transferred, **kwargs):
        src, dest = self._get_src_dest(future)
        progress_result = ProgressResult(
            self.TRANSFER_TYPE, src, dest, bytes_transferred, future.meta.size)
        self._result_queue.put(progress_result)

    def on_done(self, future, **kwargs):
        src, dest = self._get_src_dest(future)
        try:
            future.result()
            self._result_queue.put(
                SuccessResult(self.TRANSFER_TYPE, src, dest))
        except Exception as e:
            self._result_queue.put(
                FailureResult(self.TRANSFER_TYPE, src, dest, e))

    def _get_src_dest(self, future):
        raise NotImplementedError('must implement _get_src_dest()')


class UploadResultSubscriber(BaseResultSubscriber):
    TRANSFER_TYPE = 'upload'

    def _get_src_dest(self, future):
        call_args = future.meta.call_args
        src = self._get_src(call_args.fileobj)
        dest = 's3://' + call_args.bucket + '/' + call_args.key
        return src, dest

    def _get_src(self, fileobj):
        return relative_path(fileobj)


class UploadStreamResultSubscriber(UploadResultSubscriber):
    def _get_src(self, fileobj):
        return '-'


class DownloadResultSubscriber(BaseResultSubscriber):
    TRANSFER_TYPE = 'download'

    def _get_src_dest(self, future):
        call_args = future.meta.call_args
        src = 's3://' + call_args.bucket + '/' + call_args.key
        dest = self._get_dest(call_args.fileobj)
        return src, dest

    def _get_dest(self, fileobj):
        return relative_path(fileobj)


class DownloadStreamResultSubscriber(DownloadResultSubscriber):
    def _get_dest(self, fileobj):
        return '-'


class CopyResultSubscriber(BaseResultSubscriber):
    TRANSFER_TYPE = 'copy'

    def _get_src_dest(self, future):
        call_args = future.meta.call_args
        copy_source = call_args.copy_source
        src = 's3://' + copy_source['Bucket'] + '/' + copy_source['Key']
        dest = 's3://' + call_args.bucket + '/' + call_args.key
        return src, dest


class ResultRecorder(object):
    """Records and track transfer statistics based on results receieved"""
    def __init__(self):
        self.bytes_transferred = 0
        self.files_transferred = 0
        self.files_failed = 0
        self.files_warned = 0

        self.expected_bytes_transferred = 0
        self.expected_files_transferred = 0

        self._result_handler_map = {
            QueuedResult: self._record_queued_result,
            ProgressResult: self._record_progress_result,
            SuccessResult: self._record_success_result,
            FailureResult: self._record_failure_result,
            WarningResult: self._record_warning_result,
        }

    def record_result(self, result):
        """Record the result of an individual Result object"""
        self._result_handler_map.get(type(result), self._record_noop)(
            result=result)

    def _record_noop(self, result, **kwargs):
        # If the result does not have a handler, then do nothing with it.
        pass

    def _record_queued_result(self, result, **kwargs):
        self.expected_files_transferred += 1
        self.expected_bytes_transferred += result.total_transfer_size

    def _record_progress_result(self, result, **kwargs):
        self.bytes_transferred += result.bytes_transferred

    def _record_success_result(self, **kwargs):
        self.files_transferred += 1

    def _record_failure_result(self, **kwargs):
        self.files_failed += 1
        self.files_transferred += 1

    def _record_warning_result(self, **kwargs):
        self.files_warned += 1


class ResultPrinter(object):
    PROGRESS_FORMAT = (
        'Transferred {bytes_transferred}/{expected_bytes_transferred} for '
        '{files_transferred}/{expected_files_transferred} files.'
    )
    SUCCESS_FORMAT = (
        '{transfer_type}: {direction}'
    )
    FAILURE_FORMAT = (
        '{transfer_type} failed: {direction} {exception}'
    )
    WARNING_FORMAT = (
        'warning: {message}'
    )
    TWO_WAY_DIRECTION_FORMAT = '{src} to {dest}'

    def __init__(self, result_recorder, out_file=sys.stdout,
                 error_file=sys.stderr):
        """Prints status of ongoing transfer

        :type result_recorder: ResultRecorder
        :param result_recorder: The associated result recorder


        :type only_show_errors: bool
        :param only_show_errors: True if to only print out errors. Otherwise,
            print out everything.

        :type out_file: file-like obj
        :param out_file: Location to write progress and success statements

        :type error_file: file-like obj
        :param error_file: Location to write warnings and errors
        """
        self._result_recorder = result_recorder
        self._out_file = out_file
        self._error_file = error_file
        self._progress_length = 0
        self._result_handler_map = {
            ProgressResult: self._print_progress,
            SuccessResult: self._print_success,
            FailureResult: self._print_failure,
            WarningResult: self._print_warning,
        }

    def print_result(self, result):
        """Print the progress of the ongoing transfer based on a result"""
        self._result_handler_map.get(type(result), self._print_noop)(
            result=result)

    def _print_noop(self, **kwargs):
        # If the result does not have a handler, then do nothing with it.
        pass

    def _print_success(self, result, **kwargs):
        direction = self._get_direction(result)
        success_statement = self.SUCCESS_FORMAT.format(
            transfer_type=result.transfer_type, direction=direction)
        success_statement = self._adjust_statement_padding(success_statement)
        self._print_to_out_file(success_statement)
        self._redisplay_progress()

    def _print_failure(self, result, **kwargs):
        direction = self._get_direction(result)
        failure_statement = self.FAILURE_FORMAT.format(
            transfer_type=result.transfer_type, direction=direction,
            exception=result.exception
        )
        failure_statement = self._adjust_statement_padding(failure_statement)
        self._print_to_error_file(failure_statement)
        self._redisplay_progress()

    def _print_warning(self, result, **kwargs):
        warning_statement = self.WARNING_FORMAT.format(message=result.message)
        warning_statement = self._adjust_statement_padding(warning_statement)
        self._print_to_error_file(warning_statement)
        self._redisplay_progress()

    def _get_direction(self, result):
        return self.TWO_WAY_DIRECTION_FORMAT.format(
            src=result.src, dest=result.dest)

    def _redisplay_progress(self):
        # Reset to zero because done statements are printed with new lines
        # meaning there are no carriage returns to take into account when
        # printing the next line.
        self._progress_length = 0
        self._add_progress_if_needed()

    def _add_progress_if_needed(self):
        if not self._is_final_file():
            self._print_progress()

    def _print_progress(self, **kwargs):
        # Get all of the statistics in the correct form.
        bytes_transferred = human_readable_size(
            self._result_recorder.bytes_transferred)
        expected_bytes_transferred = human_readable_size(
            self._result_recorder.expected_bytes_transferred)
        files_transferred = str(self._result_recorder.files_transferred)
        expected_files_transferred = str(
            self._result_recorder.expected_files_transferred)

        # Create the display statement.
        progress_statement = self.PROGRESS_FORMAT.format(
            bytes_transferred=bytes_transferred,
            expected_bytes_transferred=expected_bytes_transferred,
            files_transferred=files_transferred,
            expected_files_transferred=expected_files_transferred
        )

        # Make sure that it overrides any previous progress bar.
        progress_statement = self._adjust_statement_padding(
                progress_statement, ending_char='\r')
        self._progress_length = len(progress_statement)

        # Print the progress out.
        self._print_to_out_file(progress_statement)

    def _adjust_statement_padding(self, print_statement, ending_char='\n'):
        print_statement = print_statement.ljust(self._progress_length, ' ')
        return print_statement + ending_char

    def _is_final_file(self):
        actual = self._result_recorder.files_transferred
        expected = self._result_recorder.expected_files_transferred
        return actual == expected

    def _print_to_out_file(self, statement):
        uni_print(statement, self._out_file)

    def _print_to_error_file(self, statement):
        uni_print(statement, self._error_file)


class OnlyShowErrorsResultPrinter(ResultPrinter):
    """A result printer that only prints out errors"""
    def _print_progress(self, **kwargs):
        pass

    def _print_success(self, result, **kwargs):
        pass


class ResultProcessor(threading.Thread):
    def __init__(self, result_queue, result_recorder, result_printer=None):
        """Thread to process results from result queue

        This includes recording statistics and printing transfer status

        :param result_queue: The result queue to process results from
        :param quiet: If True, then do not print out transfer status
        :param only_show_errors: If True, will only print out errors
        """
        threading.Thread.__init__(self)
        self._result_queue = result_queue
        self._result_recorder = result_recorder
        self._result_printer = result_printer

    def get_final_result(self):
        return CommandResult(
            self._result_recorder.files_failed,
            self._result_recorder.files_warned
        )

    def run(self):
        while True:
            try:
                result = self._result_queue.get(True)
                if isinstance(result, ShutdownThreadRequest):
                    LOGGER.debug(
                        'Shutdown request received in result processing '
                        'thread, shutting down result thread.')
                    break
                LOGGER.debug('Received result: %s', result)
                try:
                    self._process_result(result)
                except Exception as e:
                    LOGGER.debug(
                        'Error processing result: %s', e, exc_info=True)
            except queue.Empty:
                pass

    def _process_result(self, result):
        self._result_recorder.record_result(result)
        if self._result_printer:
            self._result_printer.print_result(result)
