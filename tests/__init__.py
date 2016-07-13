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
from mock import Mock, patch

from awscli.compat import six


class MockStdIn(object):
    """
    This class patches stdin in order to write a stream of bytes into
    stdin.
    """
    def __init__(self, input_bytes=b''):
        input_data = six.BytesIO(input_bytes)
        if six.PY3:
            mock_object = Mock()
            mock_object.buffer = input_data
        else:
            mock_object = input_data
        self._patch = patch('sys.stdin', mock_object)

    def __enter__(self):
        self._patch.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self._patch.__exit__()