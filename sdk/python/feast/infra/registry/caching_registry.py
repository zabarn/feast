import atexit
import logging
import threading
from abc import abstractmethod
from datetime import timedelta
from threading import Lock
from typing import List, Optional

from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.infra_object import Infra
from feast.infra.registry import proto_registry_utils
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.project_metadata import ProjectMetadata
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _utc_now

logger = logging.getLogger(__name__)


class CachingRegistry(BaseRegistry):
    def __init__(self, project: str, cache_ttl_seconds: int, cache_mode: str):
        self.cached_registry_proto = self.proto()
        proto_registry_utils.init_project_metadata(self.cached_registry_proto, project)
        self.cached_registry_proto_created = _utc_now()
        self._refresh_lock = Lock()
        self.cached_registry_proto_ttl = timedelta(
            seconds=cache_ttl_seconds if cache_ttl_seconds is not None else 0
        )
        self.cache_mode = cache_mode
        if cache_mode == "thread":
            self._start_thread_async_refresh(cache_ttl_seconds)
            atexit.register(self._exit_handler)

    @abstractmethod
    def _get_data_source(self, name: str, project: str) -> DataSource:
        pass

    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_data_source(
                self.cached_registry_proto, name, project
            )
        return self._get_data_source(name, project)

    @abstractmethod
    def _list_data_sources(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[DataSource]:
        pass

    def list_data_sources(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[DataSource]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_data_sources(
                self.cached_registry_proto, project, tags
            )
        return self._list_data_sources(project, tags)

    @abstractmethod
    def _get_entity(self, name: str, project: str) -> Entity:
        pass

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_entity(
                self.cached_registry_proto, name, project
            )
        return self._get_entity(name, project)

    @abstractmethod
    def _list_entities(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[Entity]:
        pass

    def list_entities(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Entity]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_entities(
                self.cached_registry_proto, project, tags
            )
        return self._list_entities(project, tags)

    @abstractmethod
    def _get_feature_view(self, name: str, project: str) -> FeatureView:
        pass

    def get_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_feature_view(
                self.cached_registry_proto, name, project
            )
        return self._get_feature_view(name, project)

    @abstractmethod
    def _list_feature_views(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[FeatureView]:
        pass

    def list_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_feature_views(
                self.cached_registry_proto, project, tags
            )
        return self._list_feature_views(project, tags)

    @abstractmethod
    def _get_on_demand_feature_view(
        self, name: str, project: str
    ) -> OnDemandFeatureView:
        pass

    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_on_demand_feature_view(
                self.cached_registry_proto, name, project
            )
        return self._get_on_demand_feature_view(name, project)

    @abstractmethod
    def _list_on_demand_feature_views(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[OnDemandFeatureView]:
        pass

    def list_on_demand_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[OnDemandFeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_on_demand_feature_views(
                self.cached_registry_proto, project, tags
            )
        return self._list_on_demand_feature_views(project, tags)

    @abstractmethod
    def _get_stream_feature_view(self, name: str, project: str) -> StreamFeatureView:
        pass

    def get_stream_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> StreamFeatureView:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_stream_feature_view(
                self.cached_registry_proto, name, project
            )
        return self._get_stream_feature_view(name, project)

    @abstractmethod
    def _list_stream_feature_views(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[StreamFeatureView]:
        pass

    def list_stream_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[StreamFeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_stream_feature_views(
                self.cached_registry_proto, project, tags
            )
        return self._list_stream_feature_views(project, tags)

    @abstractmethod
    def _get_feature_service(self, name: str, project: str) -> FeatureService:
        pass

    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_feature_service(
                self.cached_registry_proto, name, project
            )
        return self._get_feature_service(name, project)

    @abstractmethod
    def _list_feature_services(
        self, project: str, tags: Optional[dict[str, str]]
    ) -> List[FeatureService]:
        pass

    def list_feature_services(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureService]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_feature_services(
                self.cached_registry_proto, project, tags
            )
        return self._list_feature_services(project, tags)

    @abstractmethod
    def _get_saved_dataset(self, name: str, project: str) -> SavedDataset:
        pass

    def get_saved_dataset(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_saved_dataset(
                self.cached_registry_proto, name, project
            )
        return self._get_saved_dataset(name, project)

    @abstractmethod
    def _list_saved_datasets(self, project: str) -> List[SavedDataset]:
        pass

    def list_saved_datasets(
        self, project: str, allow_cache: bool = False
    ) -> List[SavedDataset]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_saved_datasets(
                self.cached_registry_proto, project
            )
        return self._list_saved_datasets(project)

    @abstractmethod
    def _get_validation_reference(self, name: str, project: str) -> ValidationReference:
        pass

    def get_validation_reference(
        self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.get_validation_reference(
                self.cached_registry_proto, name, project
            )
        return self._get_validation_reference(name, project)

    @abstractmethod
    def _list_validation_references(self, project: str) -> List[ValidationReference]:
        pass

    def list_validation_references(
        self, project: str, allow_cache: bool = False
    ) -> List[ValidationReference]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_validation_references(
                self.cached_registry_proto, project
            )
        return self._list_validation_references(project)

    @abstractmethod
    def _list_project_metadata(self, project: str) -> List[ProjectMetadata]:
        pass

    def list_project_metadata(
        self, project: str, allow_cache: bool = False
    ) -> List[ProjectMetadata]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            return proto_registry_utils.list_project_metadata(
                self.cached_registry_proto, project
            )
        return self._list_project_metadata(project)

    @abstractmethod
    def _get_infra(self, project: str) -> Infra:
        pass

    def get_infra(self, project: str, allow_cache: bool = False) -> Infra:
        return self._get_infra(project)

    def refresh(self, project: Optional[str] = None):
        if project:
            project_metadata = proto_registry_utils.get_project_metadata(
                registry_proto=self.cached_registry_proto, project=project
            )
            if not project_metadata:
                proto_registry_utils.init_project_metadata(
                    self.cached_registry_proto, project
                )
        self.cached_registry_proto = self.proto()
        self.cached_registry_proto_created = _utc_now()

    def _refresh_cached_registry_if_necessary(self):
        if self.cache_mode == "sync":
            with self._refresh_lock:
                expired = (
                    self.cached_registry_proto is None
                    or self.cached_registry_proto_created is None
                ) or (
                    self.cached_registry_proto_ttl.total_seconds()
                    > 0  # 0 ttl means infinity
                    and (
                        _utc_now()
                        > (
                            self.cached_registry_proto_created
                            + self.cached_registry_proto_ttl
                        )
                    )
                )
                if expired:
                    logger.info("Registry cache expired, so refreshing")
                    self.refresh()

    def _start_thread_async_refresh(self, cache_ttl_seconds):
        self.refresh()
        if cache_ttl_seconds <= 0:
            return
        self.registry_refresh_thread = threading.Timer(
            cache_ttl_seconds, self._start_thread_async_refresh, [cache_ttl_seconds]
        )
        self.registry_refresh_thread.daemon = True
        self.registry_refresh_thread.start()

    def _exit_handler(self):
        self.registry_refresh_thread.cancel()
