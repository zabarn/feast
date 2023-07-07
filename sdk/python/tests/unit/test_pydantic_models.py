# Copyright 2020 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import List

import pytest
from pydantic import BaseModel

from feast.data_source import RequestSource
from feast.entity import Entity
from feast.expediagroup.pydantic_models.data_source_model import (
    AnyDataSource,
    RequestSourceModel,
    SparkSourceModel,
)
from feast.expediagroup.pydantic_models.entity_model import EntityModel
from feast.expediagroup.pydantic_models.feature_view_model import FeatureViewModel
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.infra.registry.sql import SqlRegistry
from feast.repo_config import RegistryConfig
from feast.types import Bool, Float32


@pytest.fixture(scope="session")
def sql_registry():
    registry_config = RegistryConfig(
        registry_type="sql",
        path="sqlite://",
    )

    yield SqlRegistry(registry_config, "project_1", None)


def test_entity_apply(sql_registry):
    entity_model_json = {
        "name": "entity_1",
        "join_key": "key1",
        "value_type": 11,
        "description": "",
        "tags": {"prop1": "64", "prop2": "43"},
        "owner": "jen@project.com",
        # "created_timestamp": None,
        # "last_updated_timestamp": None
        "created_timestamp": "2023-07-03T16:05:54.678Z",
        "last_updated_timestamp": "2023-07-03T16:05:54.678Z",
    }

    entity_model = EntityModel(**entity_model_json)

    entity = entity_model.to_entity()
    sql_registry.apply_entity(entity, "project_1")

    entities = sql_registry.list_entities("project_1")
    assert len(entities) == 1
    assert entities[0].name == "entity_1"


def test_datasource_apply(sql_registry):
    spark_source_model_json = {
        "name": "spark_source_1",
        "model_type": "SparkSourceModel",
        "table": "table1",
        "query": "",
        "path": "",
        "file_format": "",
        "timestamp_field": "",
        "created_timestamp_column": "",
        "description": "",
        "owner": "",
        "date_partition_column": "",
    }

    spark_source_model = SparkSourceModel(**spark_source_model_json)

    spark_source = spark_source_model.to_data_source()
    sql_registry.apply_data_source(spark_source, "project_1")

    request_source_model_json = {
        "name": "request_source_1",
        "model_type": "RequestSourceModel",
        "schema": [{"name": "string", "dtype": "Int32", "description": "", "tags": {}}],
        "description": "desc",
        "tags": {},
        "owner": "feast",
    }

    request_source_model = RequestSourceModel(**request_source_model_json)

    request_source = request_source_model.to_data_source()
    sql_registry.apply_data_source(request_source, "project_1")

    data_sources = sql_registry.list_data_sources("project_1")
    assert len(data_sources) == 2
    assert data_sources[0].name == "spark_source_1"
    assert data_sources[1].name == "request_source_1"


def test_feature_view_apply(sql_registry):
    feature_view_model_json = {
        "name": "feature_view_1",
        "original_entities": [
            {
                "name": "entity1",
                "join_key": "key1",
                "value_type": 1,
                "description": "",
                "tags": {
                    "additionalProp1": "string",
                    "additionalProp2": "string",
                    "additionalProp3": "string",
                },
                "owner": "",
                "created_timestamp": "2023-07-03T16:05:54.678Z",
                "last_updated_timestamp": "2023-07-03T16:05:54.678Z",
            }
        ],
        "original_schema": [
            {"name": "string", "dtype": "Int32", "description": "", "tags": {}}
        ],
        "ttl": 0,
        "batch_source": {
            "name": "string",
            "model_type": "SparkSourceModel",
            "table": "table1",
            "timestamp_field": "",
            "created_timestamp_column": "",
            "field_mapping": {
                "additionalProp1": "string",
                "additionalProp2": "string",
                "additionalProp3": "string",
            },
            "description": "",
            "tags": {
                "additionalProp1": "string",
                "additionalProp2": "string",
                "additionalProp3": "string",
            },
            "owner": "",
            "date_partition_column": "",
        },
        "stream_source": None,
        "online": True,
        "description": "",
        "tags": {
            "additionalProp1": "string",
            "additionalProp2": "string",
            "additionalProp3": "string",
        },
        "owner": "",
    }

    feature_view_model = FeatureViewModel(**feature_view_model_json)

    feature_view = feature_view_model.to_feature_view()
    sql_registry.apply_feature_view(feature_view, "project_1")

    feature_views = sql_registry.list_feature_views("project_1")
    assert len(feature_views) == 1
    assert feature_views[0].name == "feature_view_1"


def test_datasource_child_deserialization():
    class DataSourcesByWire(BaseModel):
        source_models: List[AnyDataSource] = []

        class Config:
            arbitrary_types_allowed = True
            extra = "allow"

    spark_source_model_json = {
        "name": "spark_source_1",
        "model_type": "SparkSourceModel",
        "table": "table1",
        "query": "",
        "path": "",
        "file_format": "",
        "timestamp_field": "",
        "created_timestamp_column": "",
        "description": "",
        "owner": "",
        "date_partition_column": "",
    }

    spark_source_model = SparkSourceModel(**spark_source_model_json)

    request_source_model_json = {
        "name": "request_source_1",
        "model_type": "RequestSourceModel",
        "schema": [{"name": "string", "dtype": "Int32", "description": "", "tags": {}}],
        "description": "desc",
        "tags": {},
        "owner": "feast",
    }

    request_source_model = RequestSourceModel(**request_source_model_json)

    data_dict = {"source_models": [spark_source_model, request_source_model]}

    sources = DataSourcesByWire(**data_dict)

    assert type(sources.source_models[0]).__name__ == "SparkSourceModel"
    assert sources.source_models[0] == spark_source_model
    assert type(sources.source_models[1]).__name__ == "RequestSourceModel"
    assert sources.source_models[1] == request_source_model


def test_idempotent_entity_conversion():
    entity = Entity(
        name="entity_1",
        description="My entity",
        tags={"key1": "val1", "key2": "val2"},
    )
    entity_model = EntityModel.from_entity(entity)
    entity_b = entity_model.to_entity()
    assert entity == entity_b


def test_idempotent_requestsource_conversion():
    schema = [
        Field(name="f1", dtype=Float32),
        Field(name="f2", dtype=Bool),
    ]
    request_source = RequestSource(
        name="request_source_1",
        schema=schema,
        description="desc",
        tags={},
        owner="feast",
    )
    request_source_model = RequestSourceModel.from_data_source(request_source)
    request_source_b = request_source_model.to_data_source()
    assert request_source == request_source_b


def test_idempotent_sparksource_conversion():
    spark_source = SparkSource(
        name="spark_source_1",
        table="thingy",
        description="desc",
        tags={},
        owner="feast",
    )
    spark_source_model = SparkSourceModel.from_data_source(spark_source)
    spark_source_b = spark_source_model.to_data_source()
    assert spark_source == spark_source_b


def test_idempotent_featureview_conversion():
    schema = [
        Field(name="f1", dtype=Float32),
        Field(name="f2", dtype=Bool),
    ]
    user_entity = Entity(name="user1", join_keys=["user_id"])
    request_source = RequestSource(
        name="request_source_1",
        schema=schema,
        description="desc",
        tags={},
        owner="feast",
    )
    feature_view = FeatureView(
        name="feature_view_1",
        entities=[user_entity],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=request_source,
    )
    feature_view_model = FeatureViewModel.from_feature_view(feature_view)
    feature_view_b = feature_view_model.to_feature_view()
    assert feature_view == feature_view_b

    spark_source = SparkSource(
        name="sparky_sparky_boom_man",
        path="/data/driver_hourly_stats",
        file_format="parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    feature_view = FeatureView(
        name="feature_view_2",
        entities=[user_entity],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=spark_source,
    )
    feature_view_model = FeatureViewModel.from_feature_view(feature_view)
    feature_view_b = feature_view_model.to_feature_view()
    assert feature_view == feature_view_b
