"""
Pydantic Model for Data Source

Copyright 2023 Expedia Group
Author: matcarlin@expediagroup.com
"""
import sys
from datetime import timedelta
from json import dumps
from typing import Callable, Dict, List, Optional

import dill
from pydantic import BaseModel
from typing_extensions import Self

from feast.data_source import DataSource
from feast.entity import Entity
from feast.expediagroup.pydantic_models.data_source_model import (
    AnyDataSource,
    RequestSourceModel,
    SparkSourceModel,
)
from feast.expediagroup.pydantic_models.entity_model import EntityModel
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.types import ComplexFeastType, PrimitiveFeastType

SUPPORTED_DATA_SOURCES = [RequestSourceModel, SparkSourceModel]


class BaseFeatureViewModel(BaseModel):
    """
    Pydantic Model of a Feast BaseFeatureView.
    """


class FeatureViewModel(BaseFeatureViewModel):
    """
    Pydantic Model of a Feast FeatureView.
    """

    name: str
    original_entities: List[EntityModel] = []
    original_schema: Optional[List[Field]] = None
    ttl: Optional[timedelta]
    batch_source: AnyDataSource
    stream_source: Optional[AnyDataSource]
    online: bool = True
    description: str = ""
    tags: Optional[Dict[str, str]] = None
    owner: str = ""

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
        json_encoders: Dict[object, Callable] = {
            Field: lambda v: int(dumps(v.value, default=str)),
            DataSource: lambda v: v.to_pydantic_model(),
            Entity: lambda v: v.to_pydantic_model(),
            ComplexFeastType: lambda v: str(v),
            PrimitiveFeastType: lambda v: str(v),
            timedelta: lambda v: v.total_seconds() if v else 0,
        }

    def to_feature_view(self) -> FeatureView:
        """
        Given a Pydantic FeatureViewModel, create and return a FeatureView.

        Returns:
            A FeatureView.
        """
        # Convert each of the sources if they exist
        batch_source = self.batch_source.to_data_source() if self.batch_source else None
        stream_source = (
            self.stream_source.to_data_source() if self.stream_source else None
        )

        # Mirror the stream/batch source conditions in the FeatureView
        # constructor; one source is passed, either a stream source
        # which contains a batch source inside it, or a batch source
        # on its own.
        source = stream_source if stream_source else batch_source
        if stream_source:
            source.batch_source = batch_source

        # Create the FeatureView
        feature_view = FeatureView(
            name=self.name,
            source=source,
            schema=self.original_schema,
            entities=[entity.to_entity() for entity in self.original_entities],
            ttl=self.ttl,
            online=self.online,
            description=self.description,
            tags=self.tags if self.tags else None,
            owner=self.owner,
        )

        return feature_view

    @classmethod
    def from_feature_view(
        cls,
        feature_view,
    ) -> Self:  # type: ignore
        """
        Converts a FeatureView object to its pydantic model representation.

        Returns:
            A FeatureViewModel.
        """
        batch_source = None
        if feature_view.batch_source:
            class_ = getattr(
                sys.modules[__name__],
                type(feature_view.batch_source).__name__ + "Model",
            )
            if class_ not in SUPPORTED_DATA_SOURCES:
                raise ValueError(
                    "Batch source type is not a supported data source type."
                )
            batch_source = class_.from_data_source(feature_view.batch_source)
        stream_source = None
        if feature_view.stream_source:
            class_ = getattr(
                sys.modules[__name__],
                type(feature_view.stream_source).__name__ + "Model",
            )
            if class_ not in SUPPORTED_DATA_SOURCES:
                raise ValueError(
                    "Stream source type is not a supported data source type."
                )
            stream_source = class_.from_data_source(feature_view.stream_source)
        return cls(
            name=feature_view.name,
            original_entities=[
                EntityModel.from_entity(entity)
                for entity in feature_view.original_entities
            ],
            ttl=feature_view.ttl,
            original_schema=feature_view.original_schema,
            batch_source=batch_source,
            stream_source=stream_source,
            online=feature_view.online,
            description=feature_view.description,
            tags=feature_view.tags if feature_view.tags else None,
            owner=feature_view.owner,
        )


class FeatureViewProjectionModel(BaseModel):
    """
    Pydantic Model of a Feast FeatureViewProjection.
    """

    name: str
    name_alias: Optional[str]
    features: List[Field]
    # desired_features is not used in FeatureViewProjection. So defaulting to [] in
    # conversion functions
    desired_features: List[str] = []
    join_key_map: Dict[str, str] = {}

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
        json_encoders: Dict[object, Callable] = {
            Field: lambda v: int(dumps(v.value, default=str)),
        }

    def to_feature_view_projection(self) -> FeatureViewProjection:
        return FeatureViewProjection(
            name=self.name,
            name_alias=self.name_alias,
            desired_features=self.desired_features,
            features=self.features,
            join_key_map=self.join_key_map,
        )

    @classmethod
    def from_feature_view_projection(
        cls,
        feature_view_projection,
    ) -> Self:  # type: ignore
        return cls(
            name=feature_view_projection.name,
            name_alias=feature_view_projection.name_alias,
            desired_features=feature_view_projection.desired_features,
            features=feature_view_projection.features,
            join_key_map=feature_view_projection.join_key_map,
        )


class OnDemandFeatureViewModel(BaseFeatureViewModel):
    """
    Pydantic Model of a Feast OnDemandFeatureView.
    """

    name: str
    features: List[Field]
    source_feature_view_projections: Dict[str, FeatureViewProjectionModel]
    source_request_sources: Dict[str, RequestSourceModel]
    udf: str
    udf_string: str
    description: str
    tags: Dict[str, str]
    owner: str

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
        json_encoders: Dict[object, Callable] = {
            Field: lambda v: int(dumps(v.value, default=str)),
            ComplexFeastType: lambda v: str(v),
            PrimitiveFeastType: lambda v: str(v),
        }

    def to_feature_view(self) -> OnDemandFeatureView:
        source_request_sources = dict()
        if self.source_request_sources:
            for key, feature_view_projection in self.source_request_sources.items():
                source_request_sources[key] = feature_view_projection.to_data_source()

        source_feature_view_projections = dict()
        if self.source_feature_view_projections:
            for (
                key,
                feature_view_projection,
            ) in self.source_feature_view_projections.items():
                source_feature_view_projections[
                    key
                ] = feature_view_projection.to_feature_view_projection()

        return OnDemandFeatureView(
            name=self.name,
            schema=self.features,
            sources=list(source_feature_view_projections.values())
            + list(source_request_sources.values()),
            udf=dill.loads(bytes.fromhex(self.udf)),
            udf_string=self.udf_string,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

    @classmethod
    def from_feature_view(
        cls,
        on_demand_feature_view,
    ) -> Self:  # type: ignore
        source_request_sources = dict()
        if on_demand_feature_view.source_request_sources:
            for (
                key,
                req_data_source,
            ) in on_demand_feature_view.source_request_sources.items():
                source_request_sources[key] = RequestSourceModel.from_data_source(
                    req_data_source
                )

        source_feature_view_projections = dict()
        if on_demand_feature_view.source_feature_view_projections:
            for (
                key,
                feature_view_projection,
            ) in on_demand_feature_view.source_feature_view_projections.items():
                source_feature_view_projections[
                    key
                ] = FeatureViewProjectionModel.from_feature_view_projection(
                    feature_view_projection
                )

        return cls(
            name=on_demand_feature_view.name,
            features=on_demand_feature_view.features,
            source_feature_view_projections=source_feature_view_projections,
            source_request_sources=source_request_sources,
            udf=dill.dumps(on_demand_feature_view.udf, recurse=True).hex(),
            udf_string=on_demand_feature_view.udf_string,
            description=on_demand_feature_view.description,
            tags=on_demand_feature_view.tags,
            owner=on_demand_feature_view.owner,
        )
