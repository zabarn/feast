from datetime import datetime
from json import dumps
from typing import Callable, Dict, List, Optional

from pydantic import BaseModel
from typing_extensions import Self

from feast.expediagroup.pydantic_models.feature_view_model import (
    FeatureViewProjectionModel,
)
from feast.feature_service import FeatureService
from feast.field import Field
from feast.types import ComplexFeastType, PrimitiveFeastType


class FeatureServiceModel(BaseModel):
    """
    Pydantic model for Feast FeatureService
    """

    name: str
    # _features: List[Union[FeatureViewModel, OnDemandFeatureViewModel]]
    feature_view_projections: List[FeatureViewProjectionModel]
    description: str
    tags: Dict[str, str]
    owner: str
    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None
    # TODO: logging_config option is not supported temporarily.
    # we will add this fucntionality to FeatureServiceModel in future.
    # logging_config: Optional[LoggingConfig] = None

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
        json_encoders: Dict[object, Callable] = {
            Field: lambda v: int(dumps(v.value, default=str)),
            ComplexFeastType: lambda v: str(v),
            PrimitiveFeastType: lambda v: str(v),
        }

    def to_feature_service(self) -> FeatureService:
        fs = FeatureService(
            name=self.name,
            features=[],
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        fs.feature_view_projections = [
            feature_view_projection.to_feature_view_projection()
            for feature_view_projection in self.feature_view_projections
        ]
        fs.created_timestamp = self.created_timestamp
        fs.last_updated_timestamp = self.last_updated_timestamp

        return fs

    @classmethod
    def from_feature_service(
        cls,
        feature_service,
    ) -> Self:  # type: ignore
        feature_view_projections = [
            FeatureViewProjectionModel.from_feature_view_projection(
                feature_view_projection
            )
            for feature_view_projection in feature_service.feature_view_projections
        ]

        return cls(
            name=feature_service.name,
            feature_view_projections=feature_view_projections,
            description=feature_service.description,
            tags=feature_service.tags,
            owner=feature_service.owner,
            created_timestamp=feature_service.created_timestamp,
            last_updated_timestamp=feature_service.last_updated_timestamp,
        )
