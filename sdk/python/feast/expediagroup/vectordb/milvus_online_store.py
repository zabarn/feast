import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic.typing import Literal
from pymilvus import Collection, connections, utility

from feast import Entity, RepoConfig
from feast.expediagroup.vectordb.vector_feature_view import VectorFeatureView
from feast.expediagroup.vectordb.vector_online_store import VectorOnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.usage import log_exceptions_and_usage

logger = logging.getLogger(__name__)


class MilvusOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for the Milvus online store"""

    type: Literal["milvus"] = "milvus"
    """Online store type selector"""

    alias: str = "default"
    """ alias for milvus connection"""

    host: str
    """ the host URL """

    username: str
    """ username to connect to Milvus """

    password: str
    """ password to connect to Milvus """

    port: int = 19530
    """ the port to connect to a Milvus instance. Should be the one used for GRPC (default: 19530) """


class MilvusConnectionManager:
    def __init__(self, online_config: RepoConfig):
        self.online_config = online_config

    def __enter__(self):
        # Connecting to Milvus
        logger.info(
            f"Connecting to Milvus with alias {self.online_config.alias} and host {self.online_config.host} and default port {self.online_config.port}."
        )
        connections.connect(
            host=self.online_config.host,
            username=self.online_config.username,
            password=self.online_config.password,
            use_secure=True,
        )

    def __exit__(self, exc_type, exc_value, traceback):
        # Disconnecting from Milvus
        logger.info("Closing the connection to Milvus")
        connections.disconnect(self.online_config.alias)
        logger.info("Connection Closed")
        if exc_type is not None:
            logger.error(f"An exception of type {exc_type} occurred: {exc_value}")


class MilvusOnlineStore(VectorOnlineStore):
    def online_write_batch(
        self,
        config: RepoConfig,
        table: VectorFeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7971"
        )

    def online_read(
        self,
        config: RepoConfig,
        table: VectorFeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7972"
        )

    @log_exceptions_and_usage(online_store="milvus")
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[VectorFeatureView],
        tables_to_keep: Sequence[VectorFeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        with MilvusConnectionManager(config.online_store):
            for table_to_keep in tables_to_keep:
                collection_available = utility.has_collection(table_to_keep.name)
                try:
                    if collection_available:
                        logger.error(f"Collection {table_to_keep.name} already exists.")
                    else:
                        Collection(name=table_to_keep.name, schema=table_to_keep.schema)
                        logger.info(
                            f"Collection {table_to_keep.name} has been created successfully."
                        )
                except Exception as e:
                    logger.error(f"Collection update failed due to {e}")

            for table_to_delete in tables_to_delete:
                collection_available = utility.has_collection(table_to_delete.name)
                try:
                    if collection_available:
                        utility.drop_collection(table_to_delete.name)
                        logger.info(
                            f"Collection {table_to_delete.name} has been deleted successfully."
                        )
                    else:
                        return logger.error(
                            f"Collection {table_to_delete.name} does not exist or is already deleted."
                        )
                except Exception as e:
                    logger.error(f"Collection deletion failed due to {e}")

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[VectorFeatureView],
        entities: Sequence[Entity],
    ):
        raise NotImplementedError(
            "to be implemented in https://jira.expedia.biz/browse/EAPC-7974"
        )
