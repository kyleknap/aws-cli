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
from awscli.testutils import mock
from awscli.compat import queue
from awscli.compat import StringIO
from awscli.customizations.s3.executor import ShutdownThreadRequest
from awscli.customizations.s3.results import QueuedResult
from awscli.customizations.s3.results import ProgressResult
from awscli.customizations.s3.results import SuccessResult
from awscli.customizations.s3.results import FailureResult
from awscli.customizations.s3.results import UploadResultSubscriber
from awscli.customizations.s3.results import UploadStreamResultSubscriber
from awscli.customizations.s3.results import DownloadResultSubscriber
from awscli.customizations.s3.results import DownloadStreamResultSubscriber
from awscli.customizations.s3.results import CopyResultSubscriber
from awscli.customizations.s3.results import ResultRecorder
from awscli.customizations.s3.results import ResultPrinter
from awscli.customizations.s3.results import OnlyShowErrorsResultPrinter
from awscli.customizations.s3.results import ResultProcessor
from awscli.customizations.s3.utils import relative_path
from awscli.customizations.s3.utils import WarningResult


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
        self.assertEqual(
            result,
            QueuedResult(
                transfer_type=self.transfer_type,
                src=self.src,
                dest=self.dest,
                total_transfer_size=self.size
            )
        )

    def test_on_progress(self):
        ref_bytes_transferred = 1024 * 1024  # 1MB
        self.result_subscriber.on_progress(self.future, ref_bytes_transferred)
        result = self.get_queued_result()
        self.assert_result_queue_is_empty()
        self.assertEqual(
            result,
            ProgressResult(
                transfer_type=self.transfer_type,
                src=self.src,
                dest=self.dest,
                bytes_transferred=ref_bytes_transferred,
                total_transfer_size=self.size
            )
        )

    def test_on_done_success(self):
        self.result_subscriber.on_done(self.future)
        result = self.get_queued_result()
        self.assert_result_queue_is_empty()
        self.assertEqual(
            result,
            SuccessResult(
                transfer_type=self.transfer_type,
                src=self.src,
                dest=self.dest,
            )
        )

    def test_on_done_failure(self):
        self.result_subscriber.on_done(self.failure_future)
        result = self.get_queued_result()
        self.assert_result_queue_is_empty()
        self.assertEqual(
            result,
            FailureResult(
                transfer_type=self.transfer_type,
                src=self.src,
                dest=self.dest,
                exception=self.ref_exception
            )
        )


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


class ResultRecorderTest(unittest.TestCase):
    def setUp(self):
        self.transfer_type = 'upload'
        self.src = 'file'
        self.dest = 's3://mybucket/mykey'
        self.total_transfer_size = 20 * (1024 ** 1024)  # 20MB
        self.warning_message = 'a dummy warning message'
        self.exception_message = 'a dummy exception message'
        self.exception = Exception(self.exception_message)
        self.result_recorder = ResultRecorder()

    def test_queued_result(self):
        self.result_recorder.record_result(
            QueuedResult(
                transfer_type=self.transfer_type, src=self.src,
                dest=self.dest, total_transfer_size=self.total_transfer_size
            )
        )
        self.assertEqual(
            self.result_recorder.expected_bytes_transferred,
            self.total_transfer_size
        )
        self.assertEqual(self.result_recorder.expected_files_transferred, 1)

    def test_multiple_queued_results(self):
        num_results = 5
        for i in range(num_results):
            self.result_recorder.record_result(
                QueuedResult(
                    transfer_type=self.transfer_type,
                    src=self.src + str(i),
                    dest=self.dest + str(i),
                    total_transfer_size=self.total_transfer_size
                )
            )

        self.assertEqual(
            self.result_recorder.expected_bytes_transferred,
            num_results * self.total_transfer_size
        )
        self.assertEqual(
            self.result_recorder.expected_files_transferred, num_results)

    def test_progress_result(self):
        self.result_recorder.record_result(
            QueuedResult(
                transfer_type=self.transfer_type, src=self.src,
                dest=self.dest, total_transfer_size=self.total_transfer_size
            )
        )

        bytes_transferred = 1024 * 1024  # 1MB
        self.result_recorder.record_result(
            ProgressResult(
                transfer_type=self.transfer_type, src=self.src,
                dest=self.dest, bytes_transferred=bytes_transferred,
                total_transfer_size=self.total_transfer_size
            )
        )

        self.assertEqual(
            self.result_recorder.bytes_transferred, bytes_transferred)

    def test_multiple_progress_results(self):
        self.result_recorder.record_result(
            QueuedResult(
                transfer_type=self.transfer_type, src=self.src,
                dest=self.dest, total_transfer_size=self.total_transfer_size
            )
        )

        bytes_transferred = 1024 * 1024  # 1MB
        num_results = 5
        for _ in range(num_results):
            self.result_recorder.record_result(
                ProgressResult(
                    transfer_type=self.transfer_type, src=self.src,
                    dest=self.dest, bytes_transferred=bytes_transferred,
                    total_transfer_size=self.total_transfer_size
                )
            )

        self.assertEqual(
            self.result_recorder.bytes_transferred,
            num_results * bytes_transferred
        )

    def test_success_result(self):
        self.result_recorder.record_result(
            QueuedResult(
                transfer_type=self.transfer_type, src=self.src,
                dest=self.dest, total_transfer_size=self.total_transfer_size
            )
        )

        self.result_recorder.record_result(
            SuccessResult(
                transfer_type=self.transfer_type, src=self.src,
                dest=self.dest
            )
        )
        self.assertEqual(self.result_recorder.files_transferred, 1)
        self.assertEqual(self.result_recorder.files_failed, 0)

    def test_multiple_success_results(self):
        num_results = 5
        for i in range(num_results):
            self.result_recorder.record_result(
                QueuedResult(
                    transfer_type=self.transfer_type,
                    src=self.src + str(i),
                    dest=self.dest + str(i),
                    total_transfer_size=self.total_transfer_size
                )
            )

        for i in range(num_results):
            self.result_recorder.record_result(
                SuccessResult(
                    transfer_type=self.transfer_type,
                    src=self.src + str(i),
                    dest=self.dest + str(i),
                )
            )

        self.assertEqual(self.result_recorder.files_transferred, num_results)
        self.assertEqual(self.result_recorder.files_failed, 0)

    def test_failure_result(self):
        self.result_recorder.record_result(
            QueuedResult(
                transfer_type=self.transfer_type, src=self.src,
                dest=self.dest, total_transfer_size=self.total_transfer_size
            )
        )

        self.result_recorder.record_result(
            FailureResult(
                transfer_type=self.transfer_type, src=self.src, dest=self.dest,
                exception=self.exception
            )
        )

        self.assertEqual(self.result_recorder.files_transferred, 1)
        self.assertEqual(self.result_recorder.files_failed, 1)
        self.assertEqual(
            self.result_recorder.bytes_failed_to_transfer,
            self.total_transfer_size)
        self.assertEqual(self.result_recorder.bytes_transferred, 0)

    def test_multiple_failure_results(self):
        num_results = 5
        for i in range(num_results):
            self.result_recorder.record_result(
                QueuedResult(
                    transfer_type=self.transfer_type,
                    src=self.src + str(i),
                    dest=self.dest + str(i),
                    total_transfer_size=self.total_transfer_size
                )
            )

        for i in range(num_results):
            self.result_recorder.record_result(
                FailureResult(
                    transfer_type=self.transfer_type,
                    src=self.src + str(i),
                    dest=self.dest + str(i),
                    exception=self.exception
                )
            )

        self.assertEqual(self.result_recorder.files_transferred, num_results)
        self.assertEqual(self.result_recorder.files_failed, num_results)
        self.assertEqual(
            self.result_recorder.bytes_failed_to_transfer,
            self.total_transfer_size * num_results)
        self.assertEqual(self.result_recorder.bytes_transferred, 0)

    def test_failure_result_mid_progress(self):
        self.result_recorder.record_result(
            QueuedResult(
                transfer_type=self.transfer_type, src=self.src,
                dest=self.dest, total_transfer_size=self.total_transfer_size
            )
        )

        bytes_transferred = 1024 * 1024  # 1MB
        self.result_recorder.record_result(
            ProgressResult(
                transfer_type=self.transfer_type, src=self.src,
                dest=self.dest, bytes_transferred=bytes_transferred,
                total_transfer_size=self.total_transfer_size
            )
        )

        self.result_recorder.record_result(
            FailureResult(
                transfer_type=self.transfer_type, src=self.src, dest=self.dest,
                exception=self.exception
            )
        )

        self.assertEqual(self.result_recorder.files_transferred, 1)
        self.assertEqual(self.result_recorder.files_failed, 1)
        self.assertEqual(
            self.result_recorder.bytes_failed_to_transfer,
            self.total_transfer_size - bytes_transferred)
        self.assertEqual(
            self.result_recorder.bytes_transferred, bytes_transferred)

    def test_warning_result(self):
        self.result_recorder.record_result(
            WarningResult(message=self.warning_message))
        self.assertEqual(self.result_recorder.files_warned, 1)

    def test_multiple_warning_results(self):
        num_results = 5
        for _ in range(num_results):
            self.result_recorder.record_result(
                WarningResult(message=self.warning_message))
        self.assertEqual(self.result_recorder.files_warned, num_results)

    def test_unknown_result_object(self):
        self.result_recorder.record_result(object())
        # Nothing should have been affected
        self.assertEqual(self.result_recorder.bytes_transferred, 0)
        self.assertEqual(self.result_recorder.expected_bytes_transferred, 0)
        self.assertEqual(self.result_recorder.expected_files_transferred, 0)
        self.assertEqual(self.result_recorder.files_transferred, 0)


class BaseResultPrinterTest(unittest.TestCase):
    def setUp(self):
        self.result_recorder = ResultRecorder()
        self.out_file = StringIO()
        self.error_file = StringIO()
        self.result_printer = ResultPrinter(
            result_recorder=self.result_recorder,
            out_file=self.out_file,
            error_file=self.error_file
        )

    def get_progress_result(self):
        # NOTE: The actual values are not important for the purpose
        # of printing as the ResultPrinter only looks at the type and
        # the ResultRecorder to determine what to print out on progress.
        return ProgressResult(
            transfer_type=None, src=None, dest=None, bytes_transferred=None,
            total_transfer_size=None
        )


class TestResultPrinter(BaseResultPrinterTest):
    def test_unknown_result_object(self):
        self.result_printer.print_result(object())
        # Nothing should have been printed because of it.
        self.assertEqual(self.out_file.getvalue(), '')
        self.assertEqual(self.error_file.getvalue(), '')

    def test_progress(self):
        mb = 1024 * 1024

        self.result_recorder.expected_bytes_transferred = 20 * mb
        self.result_recorder.expected_files_transferred = 4
        self.result_recorder.bytes_transferred = mb
        self.result_recorder.files_transferred = 1

        progress_result = self.get_progress_result()
        self.result_printer.print_result(progress_result)
        ref_progress_statement = (
            'Completed 1.0 MiB/20.0 MiB with 3 files remaining.\r')
        self.assertEqual(self.out_file.getvalue(), ref_progress_statement)

    def test_progress_then_more_progress(self):
        mb = 1024 * 1024

        progress_result = self.get_progress_result()

        # Add the first progress update and print it out
        self.result_recorder.expected_bytes_transferred = 20 * mb
        self.result_recorder.expected_files_transferred = 4
        self.result_recorder.bytes_transferred = mb
        self.result_recorder.files_transferred = 1

        self.result_printer.print_result(progress_result)
        ref_progress_statement = (
            'Completed 1.0 MiB/20.0 MiB with 3 files remaining.\r')
        self.assertEqual(self.out_file.getvalue(), ref_progress_statement)

        # Add the second progress update
        self.result_recorder.bytes_transferred += mb
        self.result_printer.print_result(progress_result)

        # The result should be the combination of the two
        ref_progress_statement = (
            'Completed 1.0 MiB/20.0 MiB with 3 files remaining.\r'
            'Completed 2.0 MiB/20.0 MiB with 3 files remaining.\r'
        )
        self.assertEqual(self.out_file.getvalue(), ref_progress_statement)

    def test_success(self):
        transfer_type = 'upload'
        src = 'file'
        dest = 's3://mybucket/mykey'
        success_result = SuccessResult(
            transfer_type=transfer_type, src=src, dest=dest)

        self.result_printer.print_result(success_result)

        ref_success_statement = (
            'upload: file to s3://mybucket/mykey\n'
        )
        self.assertEqual(self.out_file.getvalue(), ref_success_statement)

    def test_success_with_progress(self):
        mb = 1024 * 1024

        progress_result = self.get_progress_result()

        # Add the first progress update and print it out
        self.result_recorder.expected_bytes_transferred = 20 * mb
        self.result_recorder.expected_files_transferred = 4
        self.result_recorder.bytes_transferred = mb
        self.result_recorder.files_transferred = 1
        self.result_printer.print_result(progress_result)

        # Add a success result and print it out.
        transfer_type = 'upload'
        src = 'file'
        dest = 's3://mybucket/mykey'
        success_result = SuccessResult(
            transfer_type=transfer_type, src=src, dest=dest)

        self.result_recorder.files_transferred += 1
        self.result_printer.print_result(success_result)

        # The statement should consist of:
        # * The first progress statement
        # * The success statement
        # * And the progress again since the transfer is still ongoing
        ref_statement = (
            'Completed 1.0 MiB/20.0 MiB with 3 files remaining.\r'
            'upload: file to s3://mybucket/mykey               \n'
            'Completed 1.0 MiB/20.0 MiB with 2 files remaining.\r'
        )
        self.assertEqual(self.out_file.getvalue(), ref_statement)

    def test_failure(self):
        transfer_type = 'upload'
        src = 'file'
        dest = 's3://mybucket/mykey'
        failure_result = FailureResult(
            transfer_type=transfer_type, src=src, dest=dest,
            exception=Exception('my exception'))

        self.result_printer.print_result(failure_result)

        ref_failure_statement = (
            'upload failed: file to s3://mybucket/mykey my exception\n'
        )
        self.assertEqual(self.error_file.getvalue(), ref_failure_statement)

    def test_failure_with_progress(self):
        # Make errors and regular outprint go to the same file to track order.
        shared_file = self.out_file
        self.result_printer = ResultPrinter(
            result_recorder=self.result_recorder,
            out_file=shared_file,
            error_file=shared_file
        )

        mb = 1024 * 1024

        progress_result = self.get_progress_result()

        # Add the first progress update and print it out
        self.result_recorder.expected_bytes_transferred = 20 * mb
        self.result_recorder.expected_files_transferred = 4
        self.result_recorder.bytes_transferred = mb
        self.result_recorder.files_transferred = 1
        self.result_printer.print_result(progress_result)

        # Add a success result and print it out.
        transfer_type = 'upload'
        src = 'file'
        dest = 's3://mybucket/mykey'
        failure_result = FailureResult(
            transfer_type=transfer_type, src=src, dest=dest,
            exception=Exception('my exception'))

        self.result_recorder.bytes_failed_to_transfer = 3 * mb
        self.result_recorder.files_transferred += 1
        self.result_printer.print_result(failure_result)

        # The statement should consist of:
        # * The first progress statement
        # * The failure statement
        # * And the progress again since the transfer is still ongoing
        ref_statement = (
            'Completed 1.0 MiB/20.0 MiB with 3 files remaining.\r'
            'upload failed: file to s3://mybucket/mykey my exception\n'
            'Completed 4.0 MiB/20.0 MiB with 2 files remaining.\r'
        )
        self.assertEqual(shared_file.getvalue(), ref_statement)

    def test_warning(self):
        self.result_printer.print_result(WarningResult('my warning'))
        ref_warning_statement = 'warning: my warning\n'
        self.assertEqual(self.error_file.getvalue(), ref_warning_statement)

    def test_warning_with_progress(self):
        # Make errors and regular outprint go to the same file to track order.
        shared_file = self.out_file
        self.result_printer = ResultPrinter(
            result_recorder=self.result_recorder,
            out_file=shared_file,
            error_file=shared_file
        )

        mb = 1024 * 1024

        progress_result = self.get_progress_result()

        # Add the first progress update and print it out
        self.result_recorder.expected_bytes_transferred = 20 * mb
        self.result_recorder.expected_files_transferred = 4
        self.result_recorder.bytes_transferred = mb
        self.result_recorder.files_transferred = 1
        self.result_printer.print_result(progress_result)

        self.result_printer.print_result(WarningResult('my warning'))

        # The statement should consist of:
        # * The first progress statement
        # * The warning statement
        # * And the progress again since the transfer is still ongoing
        ref_statement = (
            'Completed 1.0 MiB/20.0 MiB with 3 files remaining.\r'
            'warning: my warning                               \n'
            'Completed 1.0 MiB/20.0 MiB with 3 files remaining.\r'
        )

        self.assertEqual(shared_file.getvalue(), ref_statement)


class TestOnlyShowErrorsResultPrinter(BaseResultPrinterTest):
    def setUp(self):
        super(TestOnlyShowErrorsResultPrinter, self).setUp()
        self.result_printer = OnlyShowErrorsResultPrinter(
            result_recorder=self.result_recorder,
            out_file=self.out_file,
            error_file=self.error_file
        )

    def test_does_not_print_progress_result(self):
        progress_result = self.get_progress_result()
        self.result_printer.print_result(progress_result)
        self.assertEqual(self.out_file.getvalue(), '')

    def test_does_not_print_sucess_result(self):
        transfer_type = 'upload'
        src = 'file'
        dest = 's3://mybucket/mykey'
        success_result = SuccessResult(
            transfer_type=transfer_type, src=src, dest=dest)

        self.result_printer.print_result(success_result)
        self.assertEqual(self.out_file.getvalue(), '')

    def test_print_failure_result(self):
        transfer_type = 'upload'
        src = 'file'
        dest = 's3://mybucket/mykey'
        failure_result = FailureResult(
            transfer_type=transfer_type, src=src, dest=dest,
            exception=Exception('my exception'))

        self.result_printer.print_result(failure_result)

        ref_failure_statement = (
            'upload failed: file to s3://mybucket/mykey my exception\n'
        )
        self.assertEqual(self.error_file.getvalue(), ref_failure_statement)

    def test_print_warnings_result(self):
        self.result_printer.print_result(WarningResult('my warning'))
        ref_warning_statement = 'warning: my warning\n'
        self.assertEqual(self.error_file.getvalue(), ref_warning_statement)


class TestResultProcessor(unittest.TestCase):
    def setUp(self):
        self.result_queue = queue.Queue()
        self.result_recorder = mock.Mock()
        self.result_printer = mock.Mock()
        self.result_processor = ResultProcessor(
            self.result_queue, self.result_recorder, self.result_printer)

        self.results_recorded = []
        self.results_printed = []

        self.result_recorder.record_result = self.results_recorded.append
        self.result_printer.print_result = self.results_printed.append

    def test_run(self):
        transfer_type = 'upload'
        src = 'src'
        dest = 'dest'
        total_transfer_size = 1024 * 1024
        results_to_process = [
            QueuedResult(transfer_type, src, dest, total_transfer_size),
            SuccessResult(transfer_type, src, dest)
        ]
        results_with_shutdown = results_to_process + [ShutdownThreadRequest()]

        for result in results_with_shutdown:
            self.result_queue.put(result)
        self.result_processor.run()

        self.assertEqual(self.results_recorded, results_to_process)
        self.assertEqual(self.results_printed, results_to_process)

    def test_run_without_result_printer(self):
        transfer_type = 'upload'
        src = 'src'
        dest = 'dest'
        total_transfer_size = 1024 * 1024
        results_to_process = [
            QueuedResult(transfer_type, src, dest, total_transfer_size),
            SuccessResult(transfer_type, src, dest)
        ]
        results_with_shutdown = results_to_process + [ShutdownThreadRequest()]

        for result in results_with_shutdown:
            self.result_queue.put(result)
        self.result_processor = ResultProcessor(
            self.result_queue, self.result_recorder)
        self.result_processor.run()

        self.assertEqual(self.results_recorded, results_to_process)
        self.assertEqual(self.results_printed, [])

    def test_exception_handled_in_loop(self):
        transfer_type = 'upload'
        src = 'src'
        dest = 'dest'
        total_transfer_size = 1024 * 1024
        results_to_process = [
            QueuedResult(transfer_type, src, dest, total_transfer_size),
            SuccessResult(transfer_type, src, dest)
        ]
        results_with_shutdown = results_to_process + [ShutdownThreadRequest()]

        for result in results_with_shutdown:
            self.result_queue.put(result)
        self.result_printer.print_result = mock.Mock()
        self.result_printer.print_result.side_effect = Exception(
            'Some raised exception')
        self.result_processor.run()

        self.assertEqual(self.results_recorded, results_to_process)
        # The exception happens in the ResultPrinter, the exception being
        # thrown should result in the ResultProcessor and ResultRecorder to
        # continue to process through the result queue despite the exception.
        self.assertEqual(self.results_printed, [])

    def test_does_not_process_results_after_shutdown(self):
        transfer_type = 'upload'
        src = 'src'
        dest = 'dest'
        total_transfer_size = 1024 * 1024
        results_to_process = [
            QueuedResult(transfer_type, src, dest, total_transfer_size),
            SuccessResult(transfer_type, src, dest)
        ]
        results_with_shutdown = results_to_process + [
            ShutdownThreadRequest(), WarningResult('my warning')]

        for result in results_with_shutdown:
            self.result_queue.put(result)
        self.result_processor.run()
        # Because a ShutdownThreadRequest was sent the processor should
        # not have processed anymore results stored after it.
        self.assertEqual(self.results_recorded, results_to_process)
        self.assertEqual(self.results_printed, results_to_process)
