# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import mock
import os
import shutil
import tempfile

from awscli.testutils import unittest
from awscli.argprocess import ParamError
from awscli.customizations import autoprompt


class TestCLIAutoPrompt(unittest.TestCase):
    def setUp(self):
        self.session = mock.Mock()
        self.prompter = mock.Mock(spec=autoprompt.AutoPrompter)
        self.argument = autoprompt.AutoPromptArgument(self.session,
                                                      self.prompter)

    def tearDown(self):
        pass

    def create_args(self, cli_auto_prompt=True):
        parsed_args = mock.Mock()
        parsed_args.cli_auto_prompt = cli_auto_prompt
        return parsed_args

    def test_add_arg_if_outfile_not_in_argtable(self):
        arg_table = {}
        autoprompt.add_auto_prompt(self.session, arg_table)
        self.assertIn('cli-auto-prompt', arg_table)
        self.assertIsInstance(arg_table['cli-auto-prompt'],
                              autoprompt.AutoPromptArgument)

    def test_register_argument_action(self):
        self.session.register.assert_any_call(
            'calling-command.*', self.argument.auto_prompt_arguments
        )

    def test_auto_prompter_not_called_if_arg_not_provided(self):
        args = self.create_args(cli_auto_prompt=False)
        self.argument.auto_prompt_arguments(
            call_parameters={},
            parsed_args=args,
            parsed_globals=None,
            event_name='calling-command.iam.create-user'
        )
        self.assertFalse(self.prompter.prompt_for_values.called)

    def test_add_to_call_parameters_no_file(self):
        parsed_args = self.create_args(cli_auto_prompt=True)
        call_parameters = {}
        self.argument.auto_prompt_arguments(
            call_parameters={},
            parsed_args=parsed_args,
            parsed_globals=None,
            event_name='calling-command.iam.create-user'
        )
        self.assertTrue(self.prompter.prompt_for_values.called)
