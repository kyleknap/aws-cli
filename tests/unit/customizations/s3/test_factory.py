# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
from awscli.testutils import unittest, mock

from botocore.session import Session
from botocore.config import Config
from s3transfer.manager import TransferManager
from s3transfer.crt import CRTTransferManager

from awscli.customizations.s3.factory import (
    ClientFactory, TransferManagerFactory
)
from awscli.customizations.s3.transferconfig import RuntimeConfig


class TestClientFactory(unittest.TestCase):
    def setUp(self):
        self.session = mock.Mock(Session)
        self.factory = ClientFactory(self.session)

    def test_create_client(self):
        self.factory.create_client()
        self.session.create_client.assert_called_with('s3')

    def test_create_client_proxies_region_name(self):
        self.factory.create_client(region_name='us-west-2')
        self.session.create_client.assert_called_with(
            's3', region_name='us-west-2')

    def test_create_client_proxies_endpoint_url(self):
        self.factory.create_client(endpoint_url='https://myendpoint')
        self.session.create_client.assert_called_with(
            's3', endpoint_url='https://myendpoint')

    def test_create_client_proxies_verify(self):
        self.factory.create_client(verify=True)
        self.session.create_client.assert_called_with('s3', verify=True)

    def test_create_client_proxies_config(self):
        config = Config()
        self.factory.create_client(config=config)
        self.session.create_client.assert_called_with('s3', config=config)

    def test_create_client_can_resolve_params(self):
        params = {
            'region': 'us-west-2',
            'endpoint_url': 'https://myendpoint',
            'verify_ssl': True,
        }
        self.factory.create_client(params=params)
        self.session.create_client.assert_called_with(
            's3', region_name='us-west-2', endpoint_url='https://myendpoint',
            verify=True
        )

    def test_create_client_can_override_params(self):
        params = {
            'region': 'original',
            'endpoint_url': None,
            'verify_ssl': None,
        }
        self.factory.create_client(params=params, region_name='override')
        self.session.create_client.assert_called_with(
            's3', region_name='override', endpoint_url=None, verify=None
        )

    def test_create_source_and_transfer_clients(self):
        source_client = object()
        transfer_client = object()
        self.session.create_client.side_effect = [
            source_client, transfer_client]
        params = {
            'region': 'us-west-2',
            'endpoint_url': 'https://myendpoint',
            'verify_ssl': True,
            'source_region': None,
        }
        ret_clients = self.factory.create_source_and_transfer_clients(params)
        expected_create_client_call = mock.call(
            's3', region_name='us-west-2', endpoint_url='https://myendpoint',
            verify=True
        )
        self.assertEqual(
            self.session.create_client.call_args_list,
            [expected_create_client_call, expected_create_client_call]
        )
        self.assertEqual(ret_clients, (source_client, transfer_client))

    def test_create_clients_sets_sigv4_for_sse_kms(self):
        params = {
            'region': 'us-west-2',
            'endpoint_url': None,
            'verify_ssl': None,
            'source_region': None,
            'sse': 'aws:kms',
        }
        self.factory.create_source_and_transfer_clients(params)
        self.assertEqual(self.session.create_client.call_count, 2)
        self.assertEqual(
            self.session.create_client.call_args_list[
                0][1]['config'].signature_version,
            's3v4'
        )
        self.assertEqual(
            self.session.create_client.call_args_list[
                1][1]['config'].signature_version,
            's3v4'
        )

    def test_create_clients_respects_source_region_for_copies(self):
        params = {
            'region': 'us-west-2',
            'endpoint_url': 'https://myendpoint',
            'verify_ssl': True,
            'source_region': 'us-west-1',
            'paths_type': 's3s3',
        }
        self.factory.create_source_and_transfer_clients(params)
        expected_source_client_call = mock.call(
            's3', region_name='us-west-1', verify=True
        )
        expected_transfer_client_call = mock.call(
            's3', region_name='us-west-2', endpoint_url='https://myendpoint',
            verify=True
        )
        self.assertEqual(
            self.session.create_client.call_args_list,
            [expected_source_client_call, expected_transfer_client_call]
        )


class TestTransferManagerFactory(unittest.TestCase):
    def setUp(self):
        self.session = mock.Mock(Session)
        self.session.get_config_variable.return_value = 'var'
        self.session.get_default_client_config.return_value = None
        self.factory = TransferManagerFactory(self.session)
        self.params = {
            'region': 'us-west-2',
            'endpoint_url': None,
            'verify_ssl': None,
        }
        self.runtime_config = self.get_runtime_config()

    def get_runtime_config(self, **kwargs):
        return RuntimeConfig().build_config(**kwargs)

    def assert_is_default_manager(self, manager):
        self.assertIsInstance(manager, TransferManager)

    def assert_is_crt_manager(self, manager):
        self.assertIsInstance(manager, CRTTransferManager)

    def test_create_transfer_manager_default(self):
        transfer_client = mock.Mock()
        self.session.create_client.return_value = transfer_client
        transfer_manager = self.factory.create_transfer_manager(
            self.params, self.runtime_config)
        self.assert_is_default_manager(transfer_manager)
        self.session.create_client.assert_called_with(
            's3', region_name='us-west-2', endpoint_url=None,
            verify=None,
        )
        self.assertIs(transfer_manager.client, transfer_client)

    def test_proxies_transfer_config_to_default_transfer_manager(self):
        MB = 1024 ** 2
        self.runtime_config = self.get_runtime_config(
            multipart_chunksize=5 * MB,
            multipart_threshold=20 * MB,
            max_concurrent_requests=20,
            max_queue_size=5000,
            max_bandwidth=10 * MB,
        )
        transfer_manager = self.factory.create_transfer_manager(
            self.params, self.runtime_config)
        self.assertEqual(transfer_manager.config.multipart_chunksize, 5 * MB)
        self.assertEqual(transfer_manager.config.multipart_threshold, 20 * MB)
        self.assertEqual(transfer_manager.config.max_request_concurrency, 20)
        self.assertEqual(transfer_manager.config.max_request_queue_size, 5000)
        self.assertEqual(transfer_manager.config.max_bandwidth, 10 * MB)
        # These configurations are hardcoded and not configurable but
        # we just want to make sure they are being set by the factory.
        self.assertEqual(
            transfer_manager.config.max_in_memory_upload_chunks, 6)
        self.assertEqual(
            transfer_manager.config.max_in_memory_upload_chunks, 6)

    def test_can_provide_botocore_client_to_default_manager(self):
        transfer_client = mock.Mock()
        transfer_manager = self.factory.create_transfer_manager(
            self.params, self.runtime_config, botocore_client=transfer_client)
        self.assert_is_default_manager(transfer_manager)
        self.session.create_client.assert_not_called()
        self.assertIs(transfer_manager.client, transfer_client)

    def test_creates_default_manager_when_explicitly_set_to_default(self):
        self.runtime_config = self.get_runtime_config(
            preferred_transfer_client='default')
        transfer_manager = self.factory.create_transfer_manager(
            self.params, self.runtime_config)
        self.assert_is_default_manager(transfer_manager)

    def test_creates_crt_manager_when_preferred_transfer_client_is_crt(self):
        self.runtime_config = self.get_runtime_config(
            preferred_transfer_client='crt')
        transfer_manager = self.factory.create_transfer_manager(
            self.params, self.runtime_config)
        self.assert_is_crt_manager(transfer_manager)

    def test_creates_default_manager_for_copies(self):
        self.params['paths_type'] = 's3s3'
        self.runtime_config = self.get_runtime_config(
            preferred_transfer_client='crt')
        transfer_manager = self.factory.create_transfer_manager(
            self.params, self.runtime_config)
        self.assert_is_default_manager(transfer_manager)

    def test_creates_default_manager_when_streaming_operation(self):
        self.params['is_stream'] = True
        self.runtime_config = self.get_runtime_config(
            preferred_transfer_client='crt')
        transfer_manager = self.factory.create_transfer_manager(
            self.params, self.runtime_config)
        self.assert_is_default_manager(transfer_manager)
