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
from datetime import timedelta
from typing import List

import pandas as pd
from pydantic import BaseModel

from feast.data_source import RequestSource
from feast.entity import Entity
from feast.expediagroup.pydantic_models.data_source_model import (
    AnyDataSource,
    RequestSourceModel,
    SparkSourceModel,
)
from feast.expediagroup.pydantic_models.entity_model import EntityModel
from feast.expediagroup.pydantic_models.feature_view_model import (
    FeatureViewModel,
    FeatureViewProjectionModel,
    OnDemandFeatureViewModel,
)
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.on_demand_feature_view import OnDemandFeatureView, on_demand_feature_view
from feast.protos.feast.core.FeatureViewProjection_pb2 import (
    FeatureViewProjection as FeatureViewProjectionProto,
)
from feast.protos.feast.core.OnDemandFeatureView_pb2 import OnDemandFeatureViewMeta
from feast.types import Bool, Float32, Float64, String
from feast.value_type import ValueType


def test_datasource_child_deserialization():
    class DataSourcesByWire(BaseModel):
        source_models: List[AnyDataSource] = []

        class Config:
            arbitrary_types_allowed = True
            extra = "allow"

    spark_source_model_json = {
        "name": "string",
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
        "name": "source",
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
        name="my-entity",
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
        name="source",
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
        name="source",
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
        name="source",
        schema=schema,
        description="desc",
        tags={},
        owner="feast",
    )
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[user_entity],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=request_source,
        ttl=0
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
        name="my-feature-view",
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


def test_idempotent_feature_view_projection_conversion():
    # Feast not using desired_features while converting to proto
    python_obj = FeatureViewProjection(
    name="example_projection",
    name_alias="alias",
    desired_features=[],
    features=[Field(name="feature1", dtype=Float64), Field(name="feature2", dtype=String),],
    join_key_map={"old_key": "new_key"},
    )
    # Convert to Pydantic model
    pydantic_obj = FeatureViewProjectionModel.from_feature_view_projection(python_obj)
    # Convert back to Python model
    converted_python_obj = pydantic_obj.to_feature_view_projection()
    assert python_obj == converted_python_obj

def test_idempotent_on_demand_feature_view_conversion():
    tags = {
        "application": "feature-store-feast-demo",
        "team": "ML Platform - Feature Lifecycle",
        "owner": "MLPlatform-FeatureLifecycle@expediagroup.com",
        "product": "unified-feature-store",
        "costCenter": "80481",
    }

    """
        Entity is a collection of semantically related features.  
    """
    hotel_entity: Entity = Entity(
        name="eg_property_id",
        description="eg_property_id",
        value_type=ValueType.INT64,
        owner="bdodla@expediagroup.com",
        tags=tags,
    )

    region_entity: Entity = Entity(
        name="region_id",
        description="region_id",
        value_type=ValueType.INT64,
        owner="bdodla@expediagroup.com",
        tags=tags,
    )

    """
        Data source refers to raw features data that users own. Feature Store 
        does not manage any of the raw underlying data but instead, oversees 
        loading this data and performing different operations on 
        the data to retrieve or serve features.

        Feast uses a time-series data model to represent data.
    """

    lodging_profile_datasource: SparkSource = SparkSource(
        name="lodging_profile",
        description="EG Lodging Profile",
        query="""select eg_property_id
                    , property_name
                    , structure_category_name
                    , latitude
                    , longitude
                    , country_code
                    , CURRENT_DATE AS event_timestamp
                from egdp_test_supply.lodging_profile_eg_v5
                WHERE eg_property_id < 100000""",
        timestamp_field="event_timestamp",
        tags=tags,
        owner="bdodla@expediagroup.com",
    )

    region_info_datasource: SparkSource = SparkSource(
        name="region_info",
        description="EG Region Info",
        path="s3a://ufs-feast-redshift-staging-test-935051678728-us-east-1/ufs_feast_demo/demo_region_info",
        file_format="parquet",
        timestamp_field="event_timestamp",
        tags=tags,
        owner="bdodla@expediagroup.com",
    )

    """
        A feature view is an object that represents a logical group 
        of time-series feature data as it is found in a data source.
    """

    lodging_profile_view: FeatureView = FeatureView(
        name="lodging_profile",
        entities=[hotel_entity],
        ttl=timedelta(days=365),
        source=lodging_profile_datasource,
        tags=tags,
        description="EG Lodging Profile",
        owner="bdodla@expediagroup.com",
        schema=[
            Field(name="property_name", dtype=String),
            Field(name="structure_category_name", dtype=String),
            Field(name="latitude", dtype=Float64),
            Field(name="longitude", dtype=Float64),
            Field(name="country_code", dtype=String),
        ],
    )

    region_info_view: FeatureView = FeatureView(
        name="region_info",
        entities=[region_entity],
        ttl=timedelta(days=365),
        source=region_info_datasource,
        tags=tags,
        description="EG Region Info",
        owner="bdodla@expediagroup.com",
        schema=[
            Field(name="regioncenterlongitude", dtype=Float64),
            Field(name="regioncenterlatitude", dtype=Float64),
            Field(name="regiontype", dtype=String),
        ],
    )

    distance_decorator = on_demand_feature_view(
    sources=[lodging_profile_view, region_info_view],
    schema=[
        Field(name="distance_in_kms", dtype=Float64)
    ],
    )

    def calculate_distance_demo_go(features_df: pd.DataFrame) -> pd.DataFrame:
        import numpy as np
        df = pd.DataFrame()
        # Haversine formula
        # Radius of earth in kilometers. Use 3956 for miles
        r = 6371

        # calculate the result
        df["distance_in_kms"] = (2 * np.arcsin(np.sqrt(np.sin(
            (np.radians(features_df["latitude"]) - np.radians(
                features_df["regioncenterlatitude"])) / 2) ** 2 + np.cos(
            np.radians(features_df["regioncenterlatitude"])) * np.cos(
            np.radians(features_df["latitude"])) * np.sin(
            (np.radians(features_df["longitude"]) - np.radians(
                features_df["regioncenterlongitude"])) / 2) ** 2)) * r)

        return df

    python_obj = distance_decorator(calculate_distance_demo_go)
    pydantic_obj = OnDemandFeatureViewModel.from_on_demand_feature_view(python_obj)
    converted_python_obj = pydantic_obj.to_on_demand_feature_view()
    assert python_obj == converted_python_obj

    feast_proto: OnDemandFeatureViewMeta = converted_python_obj.to_proto()
    python_obj_from_proto = OnDemandFeatureView.from_proto(feast_proto)
    assert python_obj_from_proto == converted_python_obj
    

