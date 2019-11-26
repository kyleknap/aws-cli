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
import json
import re
import xml.parsers.expat
import xml.dom.minidom

import prompt_toolkit
from prompt_toolkit.completion import WordCompleter

from awscli.paramfile import get_paramfile, LOCAL_PREFIX_MAP
from awscli.argprocess import ParamError
from awscli.customizations.arguments import OverrideRequiredArgsArgument
from awscli.customizations.wizard import selectmenu


def register_autoprompt(cli):
    cli.register('building-argument-table', add_auto_prompt)


def add_auto_prompt(session, argument_table, **kwargs):
    # This argument cannot support operations with streaming output which
    # is designated by the argument name `outfile`.
    if 'outfile' not in argument_table:
        prompter = AutoPrompter()
        auto_prompt_argument = AutoPromptArgument(session, prompter)
        auto_prompt_argument.add_to_arg_table(argument_table)


class AutoPromptArgument(OverrideRequiredArgsArgument):
    """This argument is a boolean to let you prompt for args.

    """
    ARG_DATA = {
        'name': 'cli-auto-prompt',
        'action': 'store_true',
        'help_text': 'Auto prompts, TODO more docs'
    }

    def __init__(self, session, prompter):
        super(AutoPromptArgument, self).__init__(session)
        self._prompter = prompter
        self._arg_table = {}
        self._original_required_args = {}

    def _register_argument_action(self):
        self._session.register(
            'calling-command.*', self.auto_prompt_arguments)
        super(AutoPromptArgument, self)._register_argument_action()

    def override_required_args(self, argument_table, args, **kwargs):
        self._arg_table = argument_table
        self._original_required_args = {
            key: value for key, value in argument_table.items()
            if value.required
        }
        super(AutoPromptArgument, self).override_required_args(
            argument_table, args, **kwargs
        )

    def auto_prompt_arguments(self, call_parameters, parsed_args,
                            parsed_globals, event_name, **kwargs):

        # Check if ``--cli-auto-prompt`` was specified in the command line.
        auto_prompt = getattr(parsed_args, 'cli_auto_prompt', False)
        if auto_prompt:
            return self._prompter.prompt_for_values(
                complete_arg_table=self._arg_table,
                required_arg_table=self._original_required_args,
                apicall_parameters=call_parameters,
                command_name_parts=['aws'] + event_name.split('.')[1:],
            )


class AutoPrompter(object):
    """Handles the logic for prompting for a set of values.

    This class focuses on prompting for values that's separate from
    the integration of this functionality into the CLI
    (e.g. AutoPromptArgument).

    """
    _SENTENCE_DELIMETERS_REGEX = re.compile(r'[.:]+')
    _LINE_BREAK_CHARS = [
        '\n',
        '\u2028'
    ]
    _QUIT_SENTINEL = object()

    def __init__(self):
        pass

    def prompt_for_values(self, complete_arg_table, required_arg_table,
                          apicall_parameters, command_name_parts):
        """Prompt for values for a given CLI command.

        :param complete_arg_table:  The arg table for the command
        :param required_arg_table: The subset of the arg table for
            required args.  The prompter ensutres that it prompts for
            all required args.
        :param apicall_parameters: The API call parameters to use for the
            given command.  The values the user enters during the prompting
            process will be added to this dictionary.
        :param command_name_parts: A list of strings of the command.
            This is used to reconstruct the final command if the user
            wants to print the command, e.g. ``['aws', 'iam', 'create-user']``.

        If the user requests that that the CLI command is printed, the
        full command will be returned as a string.  Otherwise ``None``
        is returned.

        """
        cli_command = command_name_parts[:]
        # TODO: autocomplete of enums if possible. You should be
        # able to just check the value.argument_mode.enum and then
        # use a WordCompleter() to pass to the prompt() method.
        # pull in the code from the autocompleter to do this.
        # It would also be nice to autocomplete server side values
        # but we'll need to pull in the autocompleter module.
        for name, value in required_arg_table.items():
            # completer = self._get_completer(value, event_name)
            v = prompt_toolkit.prompt(
                "--%s: " % name,
                bottom_toolbar=self._get_doc_from_arg(value),
            )
            cli_command.extend(['--%s' % name, v])
            value.add_to_params(apicall_parameters, v)
        # TODO: Remove builtin args such as 'cli-input-json',
        # 'generate-cli-skeleton' etc from complete_arg_table.
        remaining_args = {
            key: value for key, value in complete_arg_table.items() if
            key not in required_arg_table
        }
        # TODO: Remove arg from remaining_args when a user provides
        # a value.
        if remaining_args:
            while True:
                choices = list(remaining_args)
                choices = [
                    {'actual_value': key,
                     'display': self._get_doc_from_arg(value)}
                    for key, value in remaining_args.items()
                ]
                choices.append({'actual_value': self._QUIT_SENTINEL,
                                'display': '[DONE] Parameter input finished'})
                choice = selectmenu.select_menu(
                    choices, display_format=lambda x: x['display']
                )
                if choice['actual_value'] is self._QUIT_SENTINEL:
                    break
                arg = remaining_args[choice['actual_value']]
                v = prompt_toolkit.prompt("--%s: " % arg.name,
                                          bottom_toolbar=choice['display'])
                # TODO: This can actually cause an error if you're
                # parsing JSON or shorthand.  Catch it in a loop
                # and be helpful.
                arg.add_to_params(apicall_parameters, v)
                cli_command.extend(['--%s' % arg.name, v])
        choice = selectmenu.select_menu(
            [{'actual_value': 'run-command',
              'display': 'Execute CLI command'},
             {'actual_value': 'print-only',
              'display': 'Print CLI command.'}],
            display_format=lambda x: x['display']
        )['actual_value']
        if choice == 'print-only':
            print(' '.join(cli_command))
            return 0

    def _get_doc_from_arg(self, doc):
        first_sentence = self._get_comment_content_from_documentation(doc)
        return '%s: %s' % (doc.name, first_sentence)

    # TODO: Everything below is from generatecliskeleton.  See if we can share
    # code.
    def _get_comment_content_from_documentation(self, shape):
        content = shape.documentation
        content = self._strip_xml_from_documentation(content)
        # In order to avoid having the comment content too dense, we limit
        # the documentation to the first sentence.
        content = self._get_first_sentence(content)
        # There are characters that may mess up the indentation of the yaml
        # by introducing new lines. We want to ignore those in comments.
        content = self._remove_line_breaks(content)
        return content

    def _strip_xml_from_documentation(self, documentation):
        try:
            # We are surrounding the docstrings with our own tags in order
            # to make sure the dom parser will look at all elements in the
            # docstring as some docstrings may not have xml nodes that do
            # not all belong to the same root node.
            xml_doc = '<doc>%s</doc>' % documentation
            xml_dom = xml.dom.minidom.parseString(xml_doc)
        except xml.parsers.expat.ExpatError:
            return documentation
        content = []
        self._strip_xml_from_child_nodes(xml_dom, content)
        return ''.join(content)

    def _strip_xml_from_child_nodes(self, node, content):
        for child_node in node.childNodes:
            if child_node.nodeType == node.TEXT_NODE:
                content.append(child_node.data)
            else:
                self._strip_xml_from_child_nodes(child_node, content)

    def _get_first_sentence(self, content):
        content = self._SENTENCE_DELIMETERS_REGEX.split(content, 1)[0]
        if content:
            content += '.'
        return content

    def _remove_line_breaks(self, content):
        for char in self._LINE_BREAK_CHARS:
            content = content.replace(char, ' ')
        return content
