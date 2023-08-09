import logging
from dataclasses import dataclass
from typing import List, Optional

import pytest
from pymilvus import CollectionSchema, DataType, FieldSchema, connections, utility

from feast.expediagroup.vectordb.milvus_online_store import (
    MilvusConnectionManager,
    MilvusOnlineStore,
    MilvusOnlineStoreConfig,
)
from feast.field import Field
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.repo_config import RepoConfig
from tests.expediagroup.milvus_online_store_creator import MilvusOnlineStoreCreator

logging.basicConfig(level=logging.INFO)

REGISTRY = "s3://test_registry/registry.db"
PROJECT = "test_aws"
PROVIDER = "aws"
TABLE_NAME = "milvus_online_store"
REGION = "us-west-2"
HOST = "localhost"
ALIAS = "milvus"


@dataclass
class MockFeatureView:
    name: str
    schema: Optional[List[Field]]


@pytest.fixture(scope="session")
def repo_config():
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=MilvusOnlineStoreConfig(
            alias=ALIAS, host=HOST, username="abc", password="cde"
        ),
        offline_store=FileOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )


@pytest.fixture
def milvus_online_store():
    return MilvusOnlineStore()


@pytest.fixture(scope="session")
def milvus_online_setup():
    # Creating an online store through embedded Milvus for all tests in the class
    online_store_creator = MilvusOnlineStoreCreator("milvus")
    online_store_creator.create_online_store()

    yield online_store_creator

    # Tearing down the Milvus instance after all tests in the class
    online_store_creator.teardown()


class TestMilvusConnectionManager:
    def test_connection_manager(self, repo_config, caplog, milvus_online_setup, mocker):

        mocker.patch("pymilvus.connections.connect")
        with MilvusConnectionManager(repo_config.online_store):
            assert (
                f"Connecting to Milvus with alias {repo_config.online_store.alias} and host {repo_config.online_store.host} and default port {repo_config.online_store.port}."
                in caplog.text
            )

        connections.connect.assert_called_once_with(
            host=repo_config.online_store.host,
            username=repo_config.online_store.username,
            password=repo_config.online_store.password,
            use_secure=True,
        )

    def test_context_manager_exit(
        self, repo_config, caplog, milvus_online_setup, mocker
    ):
        # Create a mock for connections.disconnect
        mock_disconnect = mocker.patch("pymilvus.connections.disconnect")

        # Create a mock logger to capture log calls
        mock_logger = mocker.patch(
            "feast.expediagroup.vectordb.milvus_online_store.logger", autospec=True
        )

        with MilvusConnectionManager(repo_config.online_store):
            print("Doing something")

        # Assert that connections.disconnect was called with the expected argument
        mock_disconnect.assert_called_once_with(repo_config.online_store.alias)

        # Assert that logger.info was called with expected messages
        expected_log_calls = [
            mocker.call("Closing the connection to Milvus"),
            mocker.call("Connection Closed"),
        ]
        mock_logger.info.assert_has_calls(expected_log_calls)

        with pytest.raises(Exception):
            with MilvusConnectionManager(repo_config.online_store):
                raise Exception("Test Exception")
        mock_logger.error.assert_called_once()


class TestMilvusOnlineStore:

    collection_to_write = "Collection2"
    collection_to_delete = "Collection1"

    def setup_method(self, milvus_online_setup):
        # Ensuring that the collections created are dropped before the tests are run
        connections.connect(alias="default", host=HOST, port=19530)
        # Dropping collections if they exist
        if utility.has_collection(self.collection_to_delete):
            utility.drop_collection(self.collection_to_delete)
        if utility.has_collection(self.collection_to_write):
            utility.drop_collection(self.collection_to_write)
        # Closing the temporary collection to do this
        connections.disconnect("default")

    def test_milvus_update_add_collection(
        self, repo_config, milvus_online_setup, caplog
    ):
        # Creating a common schema for collection
        schema = CollectionSchema(
            fields=[
                FieldSchema(
                    "int64", DataType.INT64, description="int64", is_primary=True
                ),
                FieldSchema(
                    "float_vector", DataType.FLOAT_VECTOR, is_primary=False, dim=128
                ),
            ]
        )

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                MockFeatureView(name=self.collection_to_write, schema=schema)
            ],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )
        assert len(utility.list_collections()) == 1
        assert utility.has_collection(self.collection_to_write) is True
        assert (
            f"Collection {self.collection_to_write} has been created successfully."
            in caplog.text
        )

    def test_milvus_update_add_existing_collection(
        self, repo_config, caplog, milvus_online_setup
    ):

        # Creating a common schema for collection
        schema = CollectionSchema(
            fields=[
                FieldSchema(
                    "int64", DataType.INT64, description="int64", is_primary=True
                ),
                FieldSchema(
                    "float_vector", DataType.FLOAT_VECTOR, is_primary=False, dim=128
                ),
            ]
        )

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                MockFeatureView(name=self.collection_to_write, schema=schema)
            ],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )
        assert len(utility.list_collections()) == 1

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                MockFeatureView(name=self.collection_to_write, schema=schema)
            ],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        assert f"Collection {self.collection_to_write} already exists." in caplog.text

    def test_milvus_update_collection_with_unsupported_schema(
        self, repo_config, caplog, milvus_online_setup
    ):

        # Checking to see if Milvus will raise an exception since primary key of type FLOAT_VECTOR is unsupported.
        with pytest.raises(Exception):
            # Creating a common schema for collection
            schema = CollectionSchema(
                fields=[
                    FieldSchema(
                        "int64", DataType.INT64, description="int64", is_primary=False
                    ),
                    FieldSchema(
                        "float_vector", DataType.FLOAT_VECTOR, is_primary=True, dim=128
                    ),
                ]
            )

            MilvusOnlineStore().update(
                config=repo_config,
                tables_to_delete=[],
                tables_to_keep=[
                    MockFeatureView(name=self.collection_to_write, schema=schema)
                ],
                entities_to_delete=None,
                entities_to_keep=None,
                partial=None,
            )

    def test_milvus_update_delete_collection(
        self, repo_config, caplog, milvus_online_setup
    ):

        # Creating a common schema for collection
        schema = CollectionSchema(
            fields=[
                FieldSchema(
                    "int64", DataType.INT64, description="int64", is_primary=True
                ),
                FieldSchema(
                    "float_vector", DataType.FLOAT_VECTOR, is_primary=False, dim=128
                ),
            ]
        )

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[
                MockFeatureView(name=self.collection_to_delete, schema=schema)
            ],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[
                MockFeatureView(name=self.collection_to_delete, schema=None)
            ],
            tables_to_keep=[],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )
        assert utility.has_collection(self.collection_to_delete) is False
        assert len(utility.list_collections()) == 0
        assert (
            f"Collection {self.collection_to_delete} has been deleted successfully."
            in caplog.text
        )

    def test_milvus_update_delete_unavailable_collection(
        self, repo_config, caplog, milvus_online_setup
    ):

        MilvusOnlineStore().update(
            config=repo_config,
            tables_to_delete=[MockFeatureView(name="abc", schema=None)],
            tables_to_keep=[],
            entities_to_delete=None,
            entities_to_keep=None,
            partial=None,
        )

        assert "Collection abc does not exist or is already deleted." in caplog.text
