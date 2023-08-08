import logging
from dataclasses import dataclass
from typing import List, Optional

import pytest
from pymilvus import CollectionSchema, DataType, FieldSchema, utility
from pymilvus.client.stub import Milvus

from feast.expediagroup.vectordb.milvus_online_store import (
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


@dataclass
class MockFeatureView:
    name: str
    schema: Optional[List[Field]]


@pytest.fixture(scope="class")
def repo_config():
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=MilvusOnlineStoreConfig(host=HOST),
        offline_store=FileOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )


@pytest.fixture
def milvus_online_store():
    return MilvusOnlineStore()


@pytest.fixture(scope="class")
def milvus_online_setup():
    # Creating an online store through embedded Milvus for all tests in the class
    online_store_creator = MilvusOnlineStoreCreator("milvus")
    online_store_creator.create_online_store()

    yield online_store_creator

    # Tearing down the Milvus instance after all tests in the class
    online_store_creator.teardown()


class TestMilvusOnlineStore:

    collection_to_write = "Collection2"
    collection_to_delete = "Collection1"

    def setup_method(self, milvus_online_setup):
        # Ensuring that the collections created are dropped before the tests are run
        MilvusOnlineStoreConfig(host=HOST)
        utility.drop_collection(self.collection_to_delete)
        utility.drop_collection(self.collection_to_write)

    def test_milvus_update_add_collection(
        self, caplog, milvus_online_setup, repo_config
    ):

        MilvusOnlineStoreConfig(host=HOST)

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

        # Cleaning up
        utility.drop_collection(self.collection_to_write)

    def test_milvus_update_add_existing_collection(self, caplog, milvus_online_setup):

        MilvusOnlineStoreConfig(host=HOST)

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

        # Cleaning up
        utility.drop_collection(self.collection_to_write)

    def test_milvus_update_collection_with_unsupported_schema(
        self, caplog, milvus_online_setup
    ):

        # Checking to see if Milvus will raise an exception since primary key of type FLOAT_VECTOR is unsupported.
        with pytest.raises(Exception):
            MilvusOnlineStoreConfig(host=HOST)
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

    def test_milvus_update_delete_collection(self, caplog, milvus_online_setup):

        MilvusOnlineStoreConfig(host=HOST)

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
        # Cleaning up
        utility.drop_collection(self.collection_to_delete)

    def test_milvus_update_delete_unavailable_collection(
        self, caplog, milvus_online_setup
    ):

        MilvusOnlineStoreConfig(host=HOST)

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

        assert (
            f"Collection {self.collection_to_delete} does not exist or is already deleted."
            in caplog.text
        )
        # Cleaning up
        utility.drop_collection(self.collection_to_delete)
