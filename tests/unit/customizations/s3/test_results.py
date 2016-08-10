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
from awscli.testutils import unittest
from awscli.compat import StringIO
from awscli.compat import queue
from awscli.customizations.s3.results import QueuedResult
from awscli.customizations.s3.results import ProgressResult
from awscli.customizations.s3.results import SuccessResult
from awscli.customizations.s3.results import FailureResult
from awscli.customizations.s3.results import WarningResult
from awscli.customizations.s3.results import UploadResultSubscriber
from awscli.customizations.s3.results import UploadStreamResultSubscriber
from awscli.customizations.s3.results import DownloadResultSubscriber
from awscli.customizations.s3.results import DownloadStreamResultSubscriber
from awscli.customizations.s3.results import CopyResultSubscriber
from awscli.customizations.s3.results import ResultRecorder
from awscli.customizations.s3.results import ResultPrinter
from awscli.customizations.s3.results import OnlyShowErrorsResultPrinter
from awscli.customizations.s3.utils import relative_path


class FakeTransferFuture(object):
    def __init__(self, result=None, exception=None, meta=None):
        self._result = result
        self._exception = exception
        self.meta = meta

    def result(self):
        if self._exception:
            raise self._exception
        return self._result


class FakeTransferFutureMeta(object):
    def __init__(self, size=None, call_args=None):
        self.size = size
        self.call_args = call_args


class FakeTransferFutureCallArgs(object):
    def __init__(self, **kwargs):
        for kwarg, val in kwargs.items():
            setattr(self, kwarg, val)


class BaseResultSubscriberTest(unittest.TestCase):
    def setUp(self):
        self.result_queue = queue.Queue()

        self.bucket = 'mybucket'
        self.key = 'mykey'
        self.filename = 'myfile'
        self.size = 20 * (1024 * 1024)  # 20 MB

        self.ref_exception = Exception()
        self.set_ref_transfer_futures()

        self.src = None
        self.dest = None
        self.transfer_type = None

    def set_ref_transfer_futures(self):
        self.future = self.get_success_transfer_future('foo')
        self.failure_future = self.get_failed_transfer_future(
            self.ref_exception)

    def get_success_transfer_future(self, result):
        return self._get_transfer_future(result=result)

    def get_failed_transfer_future(self, exception):
        return self._get_transfer_future(exception=exception)

    def _get_transfer_future(self, result=None, exception=None):
        call_args = self._get_transfer_future_call_args()
        meta = FakeTransferFutureMeta(size=self.size, call_args=call_args)
        return FakeTransferFuture(
            result=result, exception=exception, meta=meta)

    def _get_transfer_future_call_args(self):
        return FakeTransferFutureCallArgs(
            fileobj=self.filename, key=self.key, bucket=self.bucket)

    def get_queued_result(self):
        return self.result_queue.get(block=False)

    def assert_result_queue_is_empty(self):
        self.assertTrue(self.result_queue.empty())

    def assert_expected_transfer_type(self, result):
        self.assertEqual(result.transfer_type, self.transfer_type)

    def assert_expected_src_and_dest(self, result):
        self.assertEqual(result.src, self.src)
        self.assertEqual(result.dest, self.dest)

    def assert_expected_total_transfer_size(self, result):
        self.assertEqual(result.total_transfer_size, self.size)

    def assert_correct_queued_result(self, result):
        self.assertIsInstance(result, QueuedResult)
        self.assert_expected_transfer_type(result)
        self.assert_expected_src_and_dest(result)
        self.assert_expected_total_transfer_size(result)

    def assert_correct_progress_result(self, result, ref_bytes_transferred):
        self.assertIsInstance(result, ProgressResult)
        self.assert_expected_transfer_type(result)
        self.assert_expected_src_and_dest(result)
        self.assertEqual(result.bytes_transferred, ref_bytes_transferred)
        self.assert_expected_total_transfer_size(result)

    def assert_correct_success_result(self, result):
        self.assertIsInstance(result, SuccessResult)
        self.assert_expected_transfer_type(result)
        self.assert_expected_src_and_dest(result)

    def assert_correct_exception_result(self, result, ref_exception):
        self.assertIsInstance(result, FailureResult)
        self.assert_expected_transfer_type(result)
        self.assert_expected_src_and_dest(result)
        self.assertEqual(result.exception, ref_exception)


class TestUploadResultSubscriber(BaseResultSubscriberTest):
    def setUp(self):
        super(TestUploadResultSubscriber, self).setUp()
        self.src = relative_path(self.filename)
        self.dest = 's3://' + self.bucket + '/' + self.key
        self.transfer_type = 'upload'
        self.result_subscriber = UploadResultSubscriber(self.result_queue)

    def test_on_queued(self):
        self.result_subscriber.on_queued(self.future)
        result = self.get_queued_result()
        self.assert_result_queue_is_empty()
        self.assert_correct_queued_result(result)

    def test_on_progress(self):
        ref_bytes_transferred = 1024 * 1024  # 1MB
        self.result_subscriber.on_progress(self.future, ref_bytes_transferred)
        result = self.get_queued_result()
        self.assert_result_queue_is_empty()
        self.assert_correct_progress_result(result, ref_bytes_transferred)

    def test_on_done_success(self):
        self.result_subscriber.on_done(self.future)
        result = self.get_queued_result()
        self.assert_result_queue_is_empty()
        self.assert_correct_success_result(result)

    def test_on_done_failure(self):
        self.result_subscriber.on_done(self.failure_future)
        result = self.get_queued_result()
        self.assert_result_queue_is_empty()
        self.assert_correct_exception_result(result, self.ref_exception)


class TestUploadStreamResultSubscriber(TestUploadResultSubscriber):
    def setUp(self):
        super(TestUploadStreamResultSubscriber, self).setUp()
        self.src = '-'
        self.result_subscriber = UploadStreamResultSubscriber(
            self.result_queue)


class TestDownloadResultSubscriber(TestUploadResultSubscriber):
    def setUp(self):
        super(TestDownloadResultSubscriber, self).setUp()
        self.src = 's3://' + self.bucket + '/' + self.key
        self.dest = relative_path(self.filename)
        self.transfer_type = 'download'
        self.result_subscriber = DownloadResultSubscriber(self.result_queue)


class TestDownloadStreamResultSubscriber(TestDownloadResultSubscriber):
    def setUp(self):
        super(TestDownloadStreamResultSubscriber, self).setUp()
        self.dest = '-'
        self.result_subscriber = DownloadStreamResultSubscriber(
            self.result_queue)


class TestCopyResultSubscriber(TestUploadResultSubscriber):
    def setUp(self):
        self.source_bucket = 'sourcebucket'
        self.source_key = 'sourcekey'
        self.copy_source = {
            'Bucket': self.source_bucket,
            'Key': self.source_key,
        }
        super(TestCopyResultSubscriber, self).setUp()
        self.src = 's3://' + self.source_bucket + '/' + self.source_key
        self.dest = 's3://' + self.bucket + '/' + self.key
        self.transfer_type = 'copy'
        self.result_subscriber = CopyResultSubscriber(self.result_queue)

    def _get_transfer_future_call_args(self):
        return FakeTransferFutureCallArgs(
            copy_source=self.copy_source, key=self.key, bucket=self.bucket)


class BaseResultUtilsTest(unittest.TestCase):
    def setUp(self):
        self.transfer_type = 'upload'
        self.src = 'file'
        self.dest = 's3://mybucket/mykey'
        self.total_transfer_size = 20 * (1024 ** 1024)  # 20MB
        self.warning_message = 'a dummy warning message'
        self.exception_message = 'a dummy exception message'
        self.exception = Exception(self.exception_message)

    def add_queued_result(self):
        self.result_queue.put(self.get_queued_result())

    def get_queued_result(self):
        return QueuedResult(
            transfer_type=self.transfer_type, src=self.src,
            dest=self.dest, total_transfer_size=self.total_transfer_size
        )

    def add_progress_result(self, bytes_transferred):
        self.result_queue.put(self.get_progress_result(bytes_transferred))

    def get_progress_result(self, bytes_transferred):
        return ProgressResult(
            transfer_type=self.transfer_type, src=self.src,
            dest=self.dest, bytes_transferred=bytes_transferred,
            total_transfer_size=self.total_transfer_size
        )

    def add_success_result(self):
        self.result_queue.put(self.get_success_result())

    def get_success_result(self):
        return SuccessResult(
            transfer_type=self.transfer_type, src=self.src, dest=self.dest)

    def add_failure_result(self):
        self.result_queue.put(self.get_failure_result())

    def get_failure_result(self):
        return FailureResult(
            transfer_type=self.transfer_type, src=self.src, dest=self.dest,
            exception=self.exception
        )

    def add_warning_result(self):
        self.result_queue.put(self.get_warning_result())

    def get_warning_result(self):
        return WarningResult(message=self.warning_message)


class ResultRecorderTest(BaseResultUtilsTest):
    def setUp(self):
        super(ResultRecorderTest, self).setUp()
        self.result_recorder = ResultRecorder()

    def test_queued_result(self):
        self.result_recorder.record_result(self.get_queued_result())
        self.assertEqual(
            self.result_recorder.expected_bytes_transferred,
            self.total_transfer_size
        )
        self.assertEqual(self.result_recorder.expected_files_transferred, 1)

    def test_multiple_queued_results(self):
        num_results = 5
        for _ in range(num_results):
            self.result_recorder.record_result(self.get_queued_result())
        self.assertEqual(
            self.result_recorder.expected_bytes_transferred,
            num_results * self.total_transfer_size
        )
        self.assertEqual(
            self.result_recorder.expected_files_transferred, num_results)

    def test_progress_result(self):
        bytes_transferred = 1024 * 1024  # 1MB
        self.result_recorder.record_result(
            self.get_progress_result(bytes_transferred))
        self.assertEqual(
            self.result_recorder.bytes_transferred, bytes_transferred)

    def test_multiple_progress_results(self):
        bytes_transferred = 1024 * 1024  # 1MB
        num_results = 5
        for _ in range(num_results):
            self.result_recorder.record_result(
                self.get_progress_result(bytes_transferred))

        self.assertEqual(
            self.result_recorder.bytes_transferred,
            num_results * bytes_transferred
        )

    def test_success_result(self):
        self.result_recorder.record_result(self.get_success_result())
        self.assertEqual(self.result_recorder.files_transferred, 1)
        self.assertEqual(self.result_recorder.files_failed, 0)

    def test_multiple_success_results(self):
        num_results = 5
        for _ in range(num_results):
            self.result_recorder.record_result(self.get_success_result())

        self.assertEqual(self.result_recorder.files_transferred, num_results)
        self.assertEqual(self.result_recorder.files_failed, 0)

    def test_failure_result(self):
        self.result_recorder.record_result(self.get_failure_result())
        self.assertEqual(self.result_recorder.files_transferred, 1)
        self.assertEqual(self.result_recorder.files_failed, 1)

    def test_multiple_failure_results(self):
        num_results = 5
        for _ in range(num_results):
            self.result_recorder.record_result(self.get_failure_result())

        self.assertEqual(self.result_recorder.files_transferred, num_results)
        self.assertEqual(self.result_recorder.files_failed, num_results)

    def test_warning_result(self):
        self.result_recorder.record_result(self.get_warning_result())
        self.assertEqual(self.result_recorder.files_warned, 1)

    def test_multiple_warning_results(self):
        num_results = 5
        for _ in range(num_results):
            self.result_recorder.record_result(self.get_warning_result())

        self.assertEqual(self.result_recorder.files_warned, num_results)

    def test_unknown_result_object(self):
        self.result_recorder.record_result(object())
        # Nothing should have been affected
        self.assertEqual(self.result_recorder.bytes_transferred, 0)
        self.assertEqual(self.result_recorder.expected_bytes_transferred, 0)
        self.assertEqual(self.result_recorder.expected_files_transferred, 0)
        self.assertEqual(self.result_recorder.files_transferred, 0)


class BaseResultPrinterTest(BaseResultUtilsTest):
    def setUp(self):
        super(BaseResultPrinterTest, self).setUp()
        self.result_recorder = ResultRecorder()
        self.out_file = StringIO()
        self.error_file = StringIO()
        self.set_result_printer()
        self.expected_files = 4
        self.expected_bytes = 1024 * 1024 * 20  # 20MB
        self.human_readable_expected_bytes = '20.0 MiB'

    def add_result_recorder_progress(self, num_bytes=0, num_files=0):
        self.set_expected_num_files_and_bytes_if_needed()
        self.result_recorder.bytes_transferred += num_bytes
        self.result_recorder.files_transferred += num_files

    def set_expected_num_files_and_bytes_if_needed(self):
        if (self.result_recorder.expected_files_transferred == 0 and
           self.result_recorder.expected_bytes_transferred == 0):
                self.result_recorder.expected_files_transferred = \
                    self.expected_files
                self.result_recorder.expected_bytes_transferred = \
                    self.expected_bytes

    def set_result_printer(self):
        raise NotImplementedError('implement set_result_printer()')

    def assert_print_output(self, expected_output):
        self.assertEqual(self.out_file.getvalue(), expected_output)

    def assert_error_output(self, expected_error_output):
        self.assertEqual(self.error_file.getvalue(), expected_error_output)

    def get_ref_progress_statement(self, bytes_transferred, files_transferred,
                                   expected_bytes_transferred=None,
                                   expected_files_transferred=None,
                                   pad_against=None):
        if expected_bytes_transferred is None:
            expected_bytes_transferred = self.human_readable_expected_bytes
        if expected_files_transferred is None:
            expected_files_transferred = self.expected_files
        progress_statement = (
            'Transferred {bytes_transferred}/{expected_bytes_transferred} for '
            '{files_transferred}/{expected_files_transferred} files.'.format(
                bytes_transferred=bytes_transferred,
                expected_bytes_transferred=expected_bytes_transferred,
                files_transferred=str(files_transferred),
                expected_files_transferred=str(expected_files_transferred)
            )
        )
        if pad_against:
            progress_statement = progress_statement.ljust(
                len(pad_against), ' ')
        return progress_statement + '\r'

    def get_ref_success_statement(self, pad_against=None):
        success_statement = '{transfer_type}: {src} to {dest}'.format(
            transfer_type=self.transfer_type, src=self.src, dest=self.dest)
        if pad_against:
            success_statement = success_statement.ljust(len(pad_against), ' ')
        return success_statement + '\n'

    def get_ref_failure_statement(self, pad_against=None):
        failure_statement = (
            '{transfer_type} failed: {src} to {dest} {exception}'.format(
                transfer_type=self.transfer_type, src=self.src, dest=self.dest,
                exception=self.exception_message)
        )

        if pad_against:
            failure_statement = failure_statement.ljust(len(pad_against), ' ')
        return failure_statement + '\n'

    def get_ref_warning_statement(self, pad_against=None):
        warning_statement = 'warning: {message}'.format(
            message=self.warning_message)

        if pad_against:
            warning_statement = warning_statement.ljust(len(pad_against), ' ')
        return warning_statement + '\n'


class TestResultPrinter(BaseResultPrinterTest):
    def set_result_printer(self):
        self.result_printer = ResultPrinter(
            result_recorder=self.result_recorder,
            out_file=self.out_file,
            error_file=self.error_file
        )

    def test_progress(self):
        num_bytes = 1024 * 1024 * 5  # 1MB
        num_files = 1
        self.add_result_recorder_progress(num_bytes, num_files)

        result = self.get_progress_result(num_bytes)
        ref_progress_statement = self.get_ref_progress_statement(
            '5.0 MiB', num_files)

        self.result_printer.print_result(result)
        self.assert_print_output(ref_progress_statement)

    def test_progress_then_more_progress(self):
        num_bytes = 1024 * 1024 * 5  # 1MB
        num_files = 1

        # Add the first progress update
        self.add_result_recorder_progress(num_bytes, num_files)
        first_progress_result = self.get_progress_result(num_bytes)
        self.result_printer.print_result(first_progress_result)

        # Add the second progress update
        self.add_result_recorder_progress(num_bytes, num_files)
        second_progress_result = self.get_progress_result(num_bytes)
        self.result_printer.print_result(second_progress_result)

        first_ref_progress_statement = self.get_ref_progress_statement(
            '5.0 MiB', num_files)
        second_ref_progress_statement = self.get_ref_progress_statement(
            '10.0 MiB', num_files * 2)

        # The result should be the combination of the two
        self.assert_print_output(
            first_ref_progress_statement + second_ref_progress_statement)

    def test_success(self):
        result = self.get_success_result()
        self.result_printer.print_result(result)
        ref_success_statement = self.get_ref_success_statement()
        self.assert_print_output(ref_success_statement)

    def test_success_with_progress(self):
        num_bytes = 1024 * 1024 * 5  # 1MB

        # Add some prior progress
        progress_result = self.get_progress_result(num_bytes)
        self.add_result_recorder_progress(num_bytes)
        prior_ref_progress_statement = self.get_ref_progress_statement(
            '5.0 MiB', 0)
        self.result_printer.print_result(progress_result)

        # Next a success result comes indicating the file is done.
        success_result = self.get_success_result()
        self.add_result_recorder_progress(num_files=1)
        ref_success_statement = self.get_ref_success_statement(
            pad_against=prior_ref_progress_statement)
        self.result_printer.print_result(success_result)

        # Since there are still more files to transfer, it should print out
        # progress after as well.
        after_ref_progress_statement = self.get_ref_progress_statement(
            '5.0 MiB', 1)

        # Assure everything that was printed consisted of previous progress
        # statement, followed by the success statement, followed by the
        # ongoing progress statement.
        self.assert_print_output(
            prior_ref_progress_statement + ref_success_statement +
            after_ref_progress_statement
        )

    def test_failure(self):
        result = self.get_failure_result()
        self.result_printer.print_result(result)
        ref_failure_statement = self.get_ref_failure_statement()
        self.assert_error_output(ref_failure_statement)

    def test_failure_with_progress(self):
        num_bytes = 1024 * 1024 * 5  # 1MB

        # Add some prior progress
        progress_result = self.get_progress_result(num_bytes)
        self.add_result_recorder_progress(num_bytes)
        prior_ref_progress_statement = self.get_ref_progress_statement(
            '5.0 MiB', 0)
        self.result_printer.print_result(progress_result)
        self.assert_print_output(prior_ref_progress_statement)

        # Next a failure result comes indicating the file is done.
        failure_result = self.get_failure_result()
        self.add_result_recorder_progress(num_files=1)
        ref_failure_statement = self.get_ref_failure_statement(
            pad_against=prior_ref_progress_statement)
        self.result_printer.print_result(failure_result)
        self.assert_error_output(ref_failure_statement)

        # Since there are still more files to transfer, it should print out
        # progress after as well.
        after_ref_progress_statement = self.get_ref_progress_statement(
            '5.0 MiB', 1)

        # The cummulative output should now consist of both progress
        # statements
        self.assert_print_output(
            prior_ref_progress_statement + after_ref_progress_statement)

    def test_warning(self):
        result = self.get_warning_result()
        self.result_printer.print_result(result)
        ref_warning_statement = self.get_ref_warning_statement()
        self.assert_error_output(ref_warning_statement)

    def test_warning_with_progress(self):
        num_bytes = 1024 * 1024 * 5  # 1MB

        # Add some prior progress
        progress_result = self.get_progress_result(num_bytes)
        self.add_result_recorder_progress(num_bytes)
        prior_ref_progress_statement = self.get_ref_progress_statement(
            '5.0 MiB', 0)
        self.result_printer.print_result(progress_result)
        self.assert_print_output(prior_ref_progress_statement)

        # Next a warning statement comes is received that needs to be printed.
        warning_result = self.get_warning_result()
        ref_warning_statement = self.get_ref_warning_statement(
            pad_against=prior_ref_progress_statement)
        self.result_printer.print_result(warning_result)
        self.assert_error_output(ref_warning_statement)

        # Since there are still more files to transfer, it should print out
        # progress after as well.
        after_ref_progress_statement = self.get_ref_progress_statement(
            '5.0 MiB', 0)

        # The cummulative output should now consist of both progress
        # statements
        self.assert_print_output(
            prior_ref_progress_statement + after_ref_progress_statement)

    def test_unknown_result_object(self):
        self.result_printer.print_result(object())
        # Nothing should have been printed because of it.
        self.assert_print_output('')
        self.assert_error_output('')


class TestOnlyShowErrorsResultPrinter(BaseResultPrinterTest):
    def set_result_printer(self):
        self.result_printer = OnlyShowErrorsResultPrinter(
            result_recorder=self.result_recorder,
            out_file=self.out_file,
            error_file=self.error_file
        )

    def assert_has_no_print_output(self):
        self.assert_print_output('')

    def test_does_not_print_progress_result(self):
        num_bytes = 1024 * 1024 * 5  # 1MB
        num_files = 1
        self.add_result_recorder_progress(num_bytes, num_files)

        result = self.get_progress_result(num_bytes)
        ref_progress_statement = self.get_ref_progress_statement(
            '5.0 MiB', num_files)

        self.result_printer.print_result(result)
        self.assert_has_no_print_output()

    def test_does_not_print_sucess_result(self):
        result = self.get_success_result()
        self.result_printer.print_result(result)
        self.assert_has_no_print_output()

    def test_print_failure_results(self):
        result = self.get_failure_result()
        self.result_printer.print_result(result)
        ref_failure_statement = self.get_ref_failure_statement()
        self.assert_error_output(ref_failure_statement)

    def test_print_warnings_result(self):
        result = self.get_warning_result()
        self.result_printer.print_result(result)
        ref_warning_statement = self.get_ref_warning_statement()
        self.assert_error_output(ref_warning_statement)


class TestResultProcessor(BaseResultUtilsTest):
    def setUp(self):
        super(TestResultProcessor, self).setUp()
        self.result_processor = ResultProcessor(
            self.result_queue, self.result_recorder)

    def start_processor(self):
        pass
