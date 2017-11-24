# Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import os
import json
import datetime

import colorama

from botocore.compat import OrderedDict

from awscli.compat import is_windows
from awscli.utils import is_a_tty
from awscli.utils import OutputStreamFactory
from awscli.customizations.commands import BasicCommand
from awscli.customizations.history.db import DatabaseRecordReader
from awscli.customizations.history.db import DatabaseConnection
from awscli.customizations.history.constants import HISTORY_FILENAME_ENV_VAR
from awscli.customizations.history.constants import DEFAULT_HISTORY_FILENAME


class ListCommand(BasicCommand):
    NAME = 'list'
    DESCRIPTION = (
        'Shows a list of previously run commands and their command_ids. '
        'Each row shows only a bare minimum of details including the '
        'command_id, date, arguments and return code. You can use the '
        '`history show` with the command_id to see more details about '
        'a particular entry.'
    )
    ARG_TABLE = [
        {'name': 'include', 'nargs': '+',
         'help_text': (
             'Specifies which events to **only** include when showing the '
             'CLI command. This argument is mutually exclusive with '
             '``--exclude``.')},
        {'name': 'exclude', 'nargs': '+',
         'help_text': (
             'Specifies which events to exclude when showing the '
             'CLI command. This argument is mutually exclusive with '
             '``--include``.')}
    ]

    def __init__(self, session, db_reader=None, output_stream_factory=None):
        super(ListCommand, self).__init__(session)
        self._db_reader = db_reader
        if db_reader is None:
            connection = DatabaseConnection(self._get_history_db_filename())
            self._db_reader = DatabaseRecordReader(connection)
        self._output_stream_factory = output_stream_factory
        if output_stream_factory is None:
            self._output_stream_factory = OutputStreamFactory()

    def _get_history_db_filename(self):
        filename = os.environ.get(
            HISTORY_FILENAME_ENV_VAR, DEFAULT_HISTORY_FILENAME)
        if not os.path.exists(filename):
            raise RuntimeError(
                'Could not locate history. Make sure cli_history is set to '
                'enabled in the ~/.aws/config file'
            )
        return filename

    def _format_time(self, timestamp):
        command_time = datetime.datetime.fromtimestamp(timestamp / 1000)
        formatted = datetime.datetime.strftime(
            command_time, '%Y-%m-%d %I:%M:%S %p')
        return formatted

    def _format_args(self, args):
        json_value = json.loads(args)
        formatted = json_value
        return formatted

    def _format_record(self, record):
        formatted = OrderedDict([
            ('Command Id', record['id_a']),
            ('Time', self._format_time(record['timestamp'])),
            ('Arguments', self._format_args(record['args'])),
            ('Return Code', record['rc'])
        ])
        return formatted

    def _run_main(self, parsed_args, parsed_globals):
        records = [self._format_record(record) for record
                   in self._db_reader.iter_all_records()]
        with self._get_output_stream('less -SR') as output_stream:
            use_color = self._should_use_color(parsed_globals)
            formatter = TableFormatter(colorize=use_color)
            formatter('history list', records, output_stream)
        return 0

    def _should_use_color(self, parsed_globals):
        if parsed_globals.color == 'on':
            return True
        elif parsed_globals.color == 'off':
            return False
        return is_a_tty and not is_windows

    def _get_output_stream(self, preferred_pager):
        if is_a_tty():
            return self._output_stream_factory.get_output_stream(
                'pager', preferred_pager=preferred_pager)
        return self._output_stream_factory.get_output_stream('stdout')


class TableCellFormatter(object):
    def __init__(self, value, space):
        self._value = value
        self._space = space

    def has_more(self):
        return len(self._value) > 0

    def format_line(self, space):
        raise NotImplementedError('pack_into')


class StringTableCellFormatter(TableCellFormatter):
    def format_line(self):
        retval = self._value
        self._value = ""
        return retval


class ArgumentTableCellFormatter(TableCellFormatter):
    def __init__(self, value, space):
        super(ArgumentTableCellFormatter, self).__init__(value, space)
        self._last_line_ended_in_param = False
        self._indented = False

    def _indent(self):
        self._space -= 4
        self._indented = True

    def _is_param(self, value):
        return value.startswith('--')

    def _does_fit(self, packed, value, space):
        extra_space = 0
        if packed:
            # If we already have content on the line we need to separate
            # it with our new content by a space so we need to consume an
            # extra unit of space.
            extra_space = 1
        return len(value) + extra_space <= space

    def _append_value(self, packed, value, space):
        if packed:
            result = packed + " " + value
        else:
            result = value
        space_consumed = len(result) - len(packed)
        new_space = space - space_consumed
        return result, new_space

    def _pack(self, packed, value, param, space):
        result, new_space = self._append_value(packed, value, space)
        self._mark_last_packed_param(param)
        return result, new_space

    def _mark_last_packed_param(self, param):
        if param:
            self._last_line_ended_in_param = True
        else:
            self._last_line_ended_in_param = False

    def _truncate_to_fit(self, candidate, space):
        truncated = "%s..." % candidate[:space-3]
        return truncated

    def _peek(self):
        if self._value:
            return self._value[0]
        return None

    def _skip_param(self, packed, candidate, space):
        # This is a parameter (--foo) which is usually followed by a
        # value, to make the output more readable each
        # paramater tries to be the first element on a new line
        # followed by its value.
        # There are three main cases here:
        #  1) The `--param value` will not fit together on the current
        #     line.
        #  2) The `--param value` will fit together in the remainder
        #     of the current line.
        #  3) The `--param` is not followed by a value. Either there is
        #     no next element, or the next element is another param.
        # In case 1 we will move the param down a line even though it
        # would fit by itself, this gives it a better chance of sharing
        # a line with its value, even if the value doesn't fit it will
        # be on the next line slightly indented making it easy to
        # identifiy.
        # In case 2 and 3 we will leave the paramater on the current
        # line since there is nothing to try and pair it with.
        next_packed, next_space = self._append_value(
            packed, candidate, space)
        next_element = self._peek()
        if next_element is None or self._is_param(next_element):
            # Handle case 3. Do nothing since it will be included by
            # default in the if fits block below.
            return False
        elif next_element is not None and self._does_fit(
                next_packed, next_element, next_space):
            # case 2 the next element fits on this line as well so
            # we can leave the current element on this line.
            return False
        else:
            # Neither of the above are true so this must be case 1 where
            # the next element existed and did not fit on the current row.
            # So we will put this param element back into the list stop
            # packing this row by breaking out of the loop.
            return True

    def format_line(self):
        space = self._space
        formatted = ""
        while self.has_more() and space > 0:
            candidate = self._value.pop(0)
            param = self._is_param(candidate)
            if self._indented and not formatted and not param and \
               self._last_line_ended_in_param:
                # If it is not a param and not starting a line then it is
                # probably a paramater value that could not fit on the same
                # line as its parameter so we will indent it a little extra
                # for clarity.
                candidate = '  %s' % candidate
            fits = self._does_fit(formatted, candidate, space)
            if param and formatted and fits:
                # If candidate is a param and there is already content on this
                # line and the parameter would fit we need to decide if we are
                # going to skip it. If we do we put it back and break out of
                # the loop.
                if self._skip_param(formatted, candidate, space):
                    self._value.insert(0, candidate)
                    break
            if fits:
                formatted, space = self._pack(
                    formatted, candidate, param, space)
            else:
                if not formatted:
                    # Nothing got formatted yet which means this candidate is
                    # too long to fit on a single line and needs to be
                    # truncated and made to fit.
                    truncated_candidate = self._truncate_to_fit(
                        candidate, space)
                    formatted, space = self._pack(
                        formatted, truncated_candidate, param, space)
                else:
                    # Naturally ran out of space and cannot fit the next
                    # candidate into this row. We will put the candidate back.
                    # Since our loop at the top checks for space and an element
                    # remaining it will loop again if we put the candidate back
                    # so we will also break here to prevent an infinite loop.
                    self._value.insert(0, candidate)
                    break
        if not self._indented:
            self._indent()
            return formatted
        else:
            return '    %s' % formatted


class TableCellFormatterFactory(object):
    _COL_TO_FORMATTER_TYPE = {
        'Command Id': StringTableCellFormatter,
        'Time': StringTableCellFormatter,
        'Arguments': ArgumentTableCellFormatter,
        'Return Code': StringTableCellFormatter,
    }

    def create(self, col, value, space):
        cls = self._COL_TO_FORMATTER_TYPE[col]
        packer = cls(value, space)
        return packer


class TableFormatter(object):
    _MAXIMUM_FIELD_WIDTH = 50
    _COMPONENT_COLORS = {
        'delim': colorama.Style.BRIGHT + colorama.Fore.BLUE
    }

    def __init__(self, colorize=True):
        self._factory = TableCellFormatterFactory()
        self._colorize = colorize
        if self._colorize:
            colorama.init(autoreset=True, strip=False)

    def __call__(self, title, records, stream):
        if records:
            self._draw_table(title, records, stream)

    def _get_color(self, key):
        return self._COMPONENT_COLORS.get(key)

    def _draw_table(self, title, records, stream):
        column_widths = self._get_column_widths(records)
        # The number of columns + 1 is the number of separators we need
        total_width = sum(column_widths.values()) + len(column_widths) + 1
        self._write_title(stream, title, column_widths, total_width)
        self._write_column_headers(stream, column_widths)
        self._write_data_rows(stream, records, column_widths)

    def _get_column_widths(self, records):
        longest_value = OrderedDict()
        record = records[0]
        for header in record.keys():
            longest_value[header] = len(header)
        for record in records:
            for header, value in record.items():
                prior_longest = longest_value[header]
                if isinstance(value, list):
                    length = len(' '.join(value))
                else:
                    length = len(value)
                longest_value[header] = min(max(prior_longest, length),
                                            self._MAXIMUM_FIELD_WIDTH)

        # Add room in each column for a space before and after the value
        for key, value in longest_value.items():
            longest_value[key] = value + 2
        return longest_value

    def _align_center(self, value, field_width):
        padding_left = int(field_width / 2) - int(len(value) / 2)
        padding_right = field_width - padding_left - len(value)
        prefix = ' ' * padding_left
        postfix = ' ' * padding_right
        aligned_value = prefix + value + postfix
        return aligned_value

    def _align_left(self, value, field_width, offset=1):
        prefix = ' ' * offset
        padding_right = field_width - len(value) - offset
        postfix = ' ' * padding_right
        aligned_value = prefix + value + postfix
        return aligned_value

    def _write_title(self, stream, title, column_widths, total_width):
        title_space = total_width - 2  # Leave space for the sides
        centered_title = self._align_center(title, title_space)
        self._write(stream, '-' * total_width + '\n', 'delim')
        self._write(stream, '|', 'delim')
        self._write(stream, centered_title)
        self._write(stream, '|\n', 'delim')
        self._write_row_footer(stream, column_widths)

    def _write_column_headers(self, stream, column_widths):
        self._write(stream, '|', 'delim')
        for header, width in column_widths.items():
            centered_header = self._align_center(header, width)
            self._write(stream, centered_header, 'value')
            self._write(stream, '|', 'delim')
        self._write(stream, '\n')
        self._write_row_footer(stream, column_widths)

    def _write_row_footer(self, stream, column_widths, delim='+'):
        self._write(stream, delim, 'delim')
        for column_width in column_widths.values():
            self._write(stream, '-' * column_width + delim, 'delim')
        self._write(stream, '\n')

    def _wrap_record_values_in_formatter(self, record, column_widths):
        new_record = OrderedDict()
        for header, value in record.items():
            width = column_widths[header]
            # Subtract 2 from the width to leave room for a space on the left
            # and the right of the actual data.
            packer = self._factory.create(header, value, width - 2)
            new_record[header] = packer
        return new_record

    def _unpack_record_row(self, record):
        row = OrderedDict()
        for header, formatter in record.items():
            value = formatter.format_line()
            row[header] = value
        return row

    def _write_data_rows(self, stream, records, column_widths):
        for record in records:
            record = self._wrap_record_values_in_formatter(
                record, column_widths)
            while any(packer.has_more() for packer in record.values()):
                row = self._unpack_record_row(record)
                self._write_data_row(stream, row, column_widths)
            self._write_row_footer(stream, column_widths)

    def _write_data_row(self, stream, row, column_widths):
        self._write(stream, '|', 'delim')
        for header, value in row.items():
            width = column_widths[header]
            aligned_value = self._align_left(value, width)
            self._write(stream, aligned_value)
            self._write(stream, '|', 'delim')
        self._write(stream, '\n')

    def _write(self, stream, value, style=None):
        if self._colorize is True and style:
            color = self._get_color(style)
            if color is not None:
                value = color + value + colorama.Style.RESET_ALL
        stream.write(value.encode('utf-8'))
