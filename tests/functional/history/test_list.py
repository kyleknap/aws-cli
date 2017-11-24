# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
from botocore.history import HistoryRecorder

from awscli.testutils import mock, create_clidriver, FileCreator
from awscli.testutils import BaseAWSCommandParamsTest
from awscli.compat import BytesIO


class TestListCommand(BaseAWSCommandParamsTest):
    def setUp(self):
        self._make_clean_history_recorder()
        super(TestListCommand, self).setUp()
        self.files = FileCreator()
        config_contents = (
            '[default]\n'
            'cli_history = enabled'
        )
        self.environ['AWS_CONFIG_FILE'] = self.files.create_file(
            'config', config_contents)
        self.environ['AWS_CLI_HISTORY_FILE'] = self.files.create_file(
            'history.db', '')
        self.driver = create_clidriver()
        # The run_cmd patches stdout with a StringIO object (similar to what
        # nose does). Therefore it will run into issues when
        # get_binary_stdout is called because it returns sys.stdout.buffer
        # for Py3 and StringIO does not have a buffer
        self.binary_stdout_patch = mock.patch(
            'awscli.customizations.history.list.get_binary_stdout')
        mock_get_binary_stdout = self.binary_stdout_patch.start()
        self.binary_stdout = BytesIO()
        mock_get_binary_stdout.return_value = self.binary_stdout

    def _make_clean_history_recorder(self):
        # This is to ensure that for each new test run the CLI is using
        # a brand new HistoryRecorder as this is global so previous test
        # runs could have injected handlers onto it as all of the tests
        # are ran in the same process.
        history_recorder = HistoryRecorder()
        patch_get_history_recorder = mock.patch(
            'botocore.history.get_global_history_recorder', history_recorder)
        get_history_recorder_mock = patch_get_history_recorder.start()
        get_history_recorder_mock.return_value = history_recorder
        self.addCleanup(patch_get_history_recorder.stop)

    def tearDown(self):
        super(TestListCommand, self).tearDown()
        self.files.remove_all()
        self.binary_stdout_patch.stop()

    def test_something(self):
        self.run_cmd('history list', expected_rc=0)
        print(self.binary_stdout.getvalue())
