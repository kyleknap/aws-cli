import argparse

from botocore.session import Session
from botocore.compat import OrderedDict

from awscli.compat import ensure_text_type
from awscli.compat import BytesIO

from awscli.testutils import unittest, mock
from awscli.customizations.history.list import ListCommand
from awscli.customizations.history.list import TableFormatter
from awscli.customizations.history.list import TableCellFormatter
from awscli.customizations.history.list import ArgumentTableCellFormatter
from awscli.customizations.history.list import StringTableCellFormatter
from awscli.customizations.history.list import TableCellFormatterFactory
from awscli.customizations.history.show import OutputStreamFactory
from awscli.customizations.history.db import DatabaseRecordReader


class TestTableCellFormatterFactory(unittest.TestCase):
    def test_factory_does_create_argument_formatter_for_arguments(self):
        formatter = TableCellFormatterFactory().create('Arguments', [], 0)
        self.assertTrue(isinstance(formatter, ArgumentTableCellFormatter))

    def test_factory_does_create_string_formatter_for_command_id(self):
        formatter = TableCellFormatterFactory().create('Command Id', '', 0)
        self.assertTrue(isinstance(formatter, StringTableCellFormatter))

    def test_factory_does_create_string_formatter_for_time(self):
        formatter = TableCellFormatterFactory().create('Time', '', 0)
        self.assertTrue(isinstance(formatter, StringTableCellFormatter))

    def test_factory_does_create_string_formatter_for_return_code(self):
        formatter = TableCellFormatterFactory().create('Return Code', '', 0)
        self.assertTrue(isinstance(formatter, StringTableCellFormatter))

    def test_factory_does_raise_key_error_for_unknown_column(self):
        with self.assertRaises(KeyError):
            TableCellFormatterFactory().create('foobar', None, None)


class TestTableCellFormatter(unittest.TestCase):
    def test_has_more_is_false_when_empty(self):
        formatter = TableCellFormatter('', 10)
        self.assertFalse(formatter.has_more())

    def test_has_more_is_true_when_not_empty(self):
        formatter = TableCellFormatter('foo', 10)
        self.assertTrue(formatter.has_more())

    def test_has_more_is_false_when_empty_list(self):
        formatter = TableCellFormatter([], 10)
        self.assertFalse(formatter.has_more())

    def test_has_more_is_true_when_not_empty_list(self):
        formatter = TableCellFormatter(['foo'], 10)
        self.assertTrue(formatter.has_more())


class TestStringTableCellFormatter(unittest.TestCase):
    def _format(self, argument, space):
        formatter = StringTableCellFormatter(argument, space)
        result = []
        while formatter.has_more():
            line = formatter.format_line()
            result.append(line)
        return result

    def test_can_format_string_smaller(self):
        formatted = self._format('foo bar baz', 15)
        self.assertEqual(formatted, ['foo bar baz'])

    def test_can_format_string_equal(self):
        formatted = self._format('foo bar baz', 11)
        self.assertEqual(formatted, ['foo bar baz'])

    def test_can_format_string_longer(self):
        formatted = self._format('foo bar baz', 10)
        self.assertEqual(formatted, ['foo bar baz'])


class TestArgumentTableCellFormatter(unittest.TestCase):
    def _format(self, argument_list, space):
        formatter = ArgumentTableCellFormatter(argument_list, space)
        result = []
        while formatter.has_more():
            line = formatter.format_line()
            result.append(line)
        return result

    def test_can_format_nothing(self):
        formatted = self._format([], 50)
        self.assertEqual(len(formatted), 0)

    def test_can_format_into_one_line_if_possible(self):
        formatted = self._format(['s3', 'ls', 's3://bucket'], 50)
        self.assertEqual(len(formatted), 1)
        line = formatted[0]
        self.assertEqual(line, 's3 ls s3://bucket')

    def test_can_format_longer_line(self):
        formatted = self._format(['configure', 'set', 'credential_process',
                                  'aws', 'keychain'], 50)
        self.assertEqual(len(formatted), 1)
        line = formatted[0]
        self.assertEqual(line, ('configure set credential_process aws '
                                'keychain'))

    def test_does_format_param_and_value_on_same_line_if_they_fit(self):
        formatted = self._format(['s3', 'ls', 's3://bucket', '--region',
                                  'us-west-2'], 50)
        self.assertEqual(len(formatted), 1)
        self.assertEqual(formatted, [
            's3 ls s3://bucket --region us-west-2'
        ])

    def test_does_split_on_space_if_too_long_and_indent(self):
        formatted = self._format(['s3', 'ls', 's3://bucket'], 15)
        self.assertEqual(len(formatted), 2)
        self.assertEqual(formatted, [
            's3 ls',
            '    s3://bucket'
        ])

    def test_does_indent_multiple_rows_after_first_one(self):
        formatted = self._format(
            ['s3', 'cp', 's3://bucket1', 's3://bucket2'], 16)
        self.assertEqual(len(formatted), 3)
        self.assertEqual(formatted, [
            's3 cp',
            '    s3://bucket1',
            '    s3://bucket2'
        ])

    def test_can_truncate_value_if_too_long(self):
        formatted = self._format(['s3', 'ls', 's3://bucket'], 14)
        self.assertEqual(len(formatted), 2)
        self.assertEqual(formatted, [
            's3 ls',
            '    s3://bu...'
        ])

    def test_does_split_normally_after_truncation(self):
        formatted = self._format(['s3', 'cp', 's3://bucket', 's3://test'], 14)
        self.assertEqual(len(formatted), 3)
        self.assertEqual(formatted, [
            's3 cp',
            '    s3://bu...',
            '    s3://test'
        ])

    def test_can_split_multiple_rows(self):
        formatted = self._format(
            ['codecommit', 'update-repository-description',
             '--repository-name', 'myrepo',
             '--repository-description', 'My description',
             '--debug'], 50)
        self.assertEqual(len(formatted), 4)
        self.assertEqual(formatted, [
            'codecommit update-repository-description',
            '    --repository-name myrepo',
            '    --repository-description My description',
            '    --debug'
        ])

    def test_does_extra_indent_param_values_if_starting_a_line(self):
        formatted = self._format(
            ['codecommit', 'update-repository-description',
             '--repository-name', 'myrepo',
             '--repository-description', 'My description',
             '--debug'], 42)
        self.assertEqual(len(formatted), 4)
        self.assertEqual(formatted, [
            'codecommit update-repository-description',
            '    --repository-name myrepo',
            '    --repository-description',
            '      My description --debug'
        ])

    def test_can_truncate_long_param_value_on_its_own_line(self):
        formatted = self._format(
            ['codecommit', 'update-repository-description',
             '--repository-name', 'myrepo',
             '--repository-description',
             'My description does not fit on one line',
             '--debug'], 42)
        self.assertEqual(len(formatted), 5)
        self.assertEqual(formatted, [
            'codecommit update-repository-description',
            '    --repository-name myrepo',
            '    --repository-description',
            '      My description does not fit on on...',
            '    --debug'
        ])

    def test_does_format_trailing_debug_into_current_line(self):
        formatted = self._format(
            ['codecommit', 'update-repository-description',
             '--repository-name', 'myrepo',
             '--repository-description',
             'foo',
             '--debug'], 42)
        self.assertEqual(len(formatted), 3)
        self.assertEqual(formatted, [
            'codecommit update-repository-description',
            '    --repository-name myrepo',
            '    --repository-description foo --debug',
        ])


class TestTableFormatter(unittest.TestCase):
    def setUp(self):
        self.output = BytesIO()
        self.table = TableFormatter(colorize=False)

    def _write_records(self, title, records):
        self.table(title, records, self.output)
        output = ensure_text_type(self.output.getvalue())
        return output

    def _make_record(self, cid, time, args, rc):
        record = OrderedDict([
            ('Command Id', cid),
            ('Time', time),
            ('Arguments', args),
            ('Return Code', str(rc))
        ])
        return record

    def test_display_nothing_for_no_records(self):
        output = self._write_records('Foobar', [])
        self.assertEqual(output, '')

    def test_display_title(self):
        output = self._write_records('Foobar', [
            self._make_record('', '', [], 0)
        ])
        self.assertIn('Foobar', output)

    def test_display_headers(self):
        output = self._write_records('Foobar', [
            self._make_record('', '', [], 0)
        ])
        self.assertIn(
            '| Command Id | Time | Arguments | Return Code |', output)

    def test_display_record_values(self):
        output = self._write_records('Foobar', [
            self._make_record('cid', 'time', ['args'], 123)
        ])
        # The values should be left aligned where possible.
        self.assertIn(
            '| cid        | time | args      | 123         |', output)

    def test_display_multiple_records(self):
        output = self._write_records('Foobar', [
            self._make_record('cid', 'time', ['args'], 123),
            self._make_record('hashvalue', 'Five thirty', ['more args'], 456)
        ])
        self.assertIn(
            '| cid        | time        | args      | 123         |', output)
        self.assertIn(
            '| hashvalue  | Five thirty | more args | 456         |', output)

    def test_display_wrap_arguments_when_too_long(self):
        output = self._write_records('Foobar', [
            self._make_record(
                'hashvalue', '123',
                ['more', 'args', 'that are pretty long', 'sometimes', 'they',
                 'will not fit', 'on one line'],
                456)
        ])
        self.assertIn(
            ('| hashvalue  | 123  | more args that are pretty long sometimes'
             ' they      | 456         |'), output)
        self.assertIn(
            ('|            |      |     will not fit on one line            '
             '           |             |'), output)


class TestListCommand(unittest.TestCase):
    def setUp(self):
        self.session = mock.Mock(Session)

        self.output_stream_factory = mock.Mock(OutputStreamFactory)

        # MagicMock is needed because it can handle context managers.
        # Normal Mock will throw AttributeErrors
        output_stream_context = mock.MagicMock()
        self.output_stream = mock.Mock()
        output_stream_context.__enter__.return_value = self.output_stream

        self.output_stream_factory.get_output_stream.return_value = \
            output_stream_context

        self.db_reader = mock.Mock(DatabaseRecordReader)
        self.db_reader.iter_all_records.return_value = []

        self.list_cmd = ListCommand(
            self.session, self.db_reader, self.output_stream_factory)

        self.parsed_args = argparse.Namespace()

        self.parsed_globals = argparse.Namespace()
        self.parsed_globals.color = 'auto'

    def _make_record(self, cid, time, args, rc):
        record = {
            'id_a': cid,
            'timestamp': time,
            'args': args,
            'rc': rc
        }
        return record

    def test_does_call_iter_all_records(self):
        self.list_cmd._run_main(self.parsed_args, self.parsed_globals)
        self.assertTrue(self.db_reader.iter_all_records.called)

    def test_list_does_write_values_to_stream(self):
        self.db_reader.iter_all_records.return_value = [
            self._make_record('abc', 1511376242067, '["s3", "ls"]', '0')
        ]
        self.list_cmd._run_main(self.parsed_args, self.parsed_globals)
        self.assertTrue(self.output_stream.write.called)
