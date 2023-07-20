import itertools
import os
import sqlite3
import struct
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic import StrictStr
from pydantic.schema import Literal

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.infra_object import SQLITE_INFRA_OBJECT_CLASS_TYPE, InfraObject
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.core.InfraObject_pb2 import InfraObject as InfraObjectProto
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.core.SqliteTable_pb2 import SqliteTable as SqliteTableProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.types import FeastType, ValueType
from feast.usage import log_exceptions_and_usage, tracing_span
from feast.utils import to_naive_utc

class VectorOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for local (SQLite-based) similarity search store"""

    type: Literal["vector"] = "vector"
    """ Online store type selector"""

    path: StrictStr = "data/online.db"
    """ (optional) Path to sqlite db """

class VectorOnlineStore(OnlineStore):
    """
    Test vector db using SQLite implementation of the online store interface. Not recommended for production usage.

    Attributes:
        _conn: SQLite connection.
    """

    _conn: Optional[sqlite3.Connection] = None
    _VALUE_TYPES_TO_SQL_TYPES: Dict[ValueType, str] = {
        ValueType.UNKNOWN: "NULL",
        ValueType.BYTES: "TEXT",
        ValueType.STRING: "BLOB",
        ValueType.INT32: "INTEGER",
        ValueType.INT64: "INTEGER",
        ValueType.DOUBLE: "REAL",
        ValueType.FLOAT: "REAL",
        ValueType.BOOL: "INTEGER",
        ValueType.UNIX_TIMESTAMP: "timestamp",
        ValueType.BYTES_LIST: "BLOB",
        ValueType.STRING_LIST: "BLOB",
        ValueType.INT32_LIST: "BLOB",
        ValueType.INT64_LIST: "BLOB",
        ValueType.DOUBLE_LIST: "BLOB",
        ValueType.FLOAT_LIST: "BLOB",
        ValueType.BOOL_LIST: "BLOB",
        ValueType.UNIX_TIMESTAMP_LIST: "BLOB",
        ValueType.NULL: "NULL"
    }

    @staticmethod
    def _get_db_path(config: RepoConfig) -> str:
        assert (
                config.online_store.type == "vector"
                or config.online_store.type.endswith("VectorOnlineStore")
        )

        if config.repo_path and not Path(config.online_store.path).is_absolute():
            db_path = str(config.repo_path / config.online_store.path)
        else:
            db_path = config.online_store.path
        return db_path

    def _get_conn(self, config: RepoConfig):
        if not self._conn:
            db_path = self._get_db_path(config)
            self._conn = _initialize_conn(db_path)
        return self._conn

    @log_exceptions_and_usage(online_store="vector")
    def online_write_batch(
            self,
            config: RepoConfig,
            table: FeatureView,
            data: List[
                Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
            ],
            progress: Optional[Callable[[int], Any]],
    ) -> None:

        conn = self._get_conn(config)

        project = config.project

        with conn:
            for entity_key, values, timestamp, created_ts in data:
                entity_key_names, entity_key_values = self._get_clean_entity_key_value(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                timestamp = to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = to_naive_utc(created_ts)

                feature_names = []
                feature_values = []
                for feature_name, val in values.items():
                    feature_names.append(feature_name)
                    feature_values.append(self._get_clean_val(val))

                conn.execute(
                    f"""
                        UPDATE {_table_id(project, table)}
                        SET {' = ?, '.join(feature_names)} = ?, event_ts = ?, created_ts = ?
                        WHERE ({' = ? AND '.join(entity_key_names)} = ?)
                    """,
                    (
                        # SET
                        *feature_values,
                        timestamp,
                        created_ts,
                        # WHERE
                        *entity_key_values,
                    ),
                )

                values_inserted = (len(entity_key_values) + len(feature_values) + 2)*('?',)
                conn.execute(
                    f"""INSERT OR IGNORE INTO {_table_id(project, table)}
                        ({', '.join(entity_key_names)}, {', '.join(feature_names)}, event_ts, created_ts)
                        VALUES ({', '.join(values_inserted)})""",
                    (
                        *entity_key_values,
                        *feature_values,
                        timestamp,
                        created_ts,
                    ),
                )
                if progress:
                    progress(1)

    @log_exceptions_and_usage(online_store="vector")
    def online_read(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKeyProto],
            requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Faking a vector similarity search"""
        conn = self._get_conn(config)
        cur = conn.cursor()

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        with tracing_span(name="remote_call"):
            entity_values = []
            for entity_key in entity_keys:
                _, v = self._get_clean_entity_key_value(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                entity_values.extend(v)

            selectors = [*entity_keys[0].join_keys, "event_ts"]
            if requested_features:
                selectors.extend(requested_features)
            else:
                selectors.append("*")
            # Fetch all entities in one go
            cur.execute(
                f"SELECT {', '.join(selectors)} "
                f"FROM {_table_id(config.project, table)} "
                f"WHERE {entity_keys[0].join_keys[0]} IN ({','.join('?' * len(entity_keys))}) "
                f"ORDER BY {entity_keys[0].join_keys[0]}",
                entity_values,
            )
            rows = cur.fetchall()

        rows = {
            k: list(group) for k, group in itertools.groupby(rows, key=lambda r: r[0])
        }
        for entity_key in entity_keys:
            _, entity_key_vals = self._get_clean_entity_key_value(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            res = {}
            res_ts = None
            row = rows.get(entity_key_vals[0], [()])[0]
            ts = row[1]
            for i, feature_name in enumerate(requested_features):
                raw_val = row[i+2]
                if isinstance(raw_val, List):
                    val = ValueProto()
                    val.ParseFromString(struct.pack("b", raw_val))
                elif isinstance(raw_val, str):
                    val = ValueProto(string_val=raw_val)
                else:
                    val = ValueProto()
                    val.ParseFromString(raw_val.bin())
                res[feature_name] = val
                res_ts = ts

            if not res:
                result.append((None, None))
            else:
                result.append((res_ts, res))
        return result

    def _get_clean_entity_key_value(self, entity_key: EntityKeyProto, entity_key_serialization_version=1):
        sorted_keys, sorted_values = zip(
            *sorted(zip(entity_key.join_keys, entity_key.entity_values))
        )
        output = []
        for v in sorted_values:
            value_type= v.WhichOneof("val")
            if value_type == "string_val":
                output.append(v.string_val)
            elif value_type == "bytes_val":
                output.append(v.bytes_val)
            elif value_type == "int32_val":
                output.append(v.int32_val)
            elif value_type == "int64_val":
                if 0 <= entity_key_serialization_version <= 1:
                    output.append(v.int64_val)
                output.append(v.int64_val)
            else:
                raise ValueError(f"Value type not supported for Firestore: {v}")
        return [x for x in sorted_keys], output

    def _get_clean_val(self, v):
        value_type = v.WhichOneof("val")
        if value_type == "string_val":
            return v.string_val
        elif value_type == "bytes_val":
            return v.bytes_val
        elif value_type == "int32_val":
            return v.int32_val
        elif value_type == "int64_val":
            return v.int64_val
        elif value_type == "double_val":
            return v.double_val
        elif value_type == "float_val":
            return v.float_val
        elif value_type == "bool_val":
            return v.bool_val
        elif value_type == "unix_timestamp_val":
            return v.unix_timestamp_val
        else:
            raise ValueError(f"Value type not supported for Firestore: {v}")

    @log_exceptions_and_usage(online_store="vector")
    def update(
            self,
            config: RepoConfig,
            tables_to_delete: Sequence[FeatureView],
            tables_to_keep: Sequence[FeatureView],
            entities_to_delete: Sequence[Entity],
            entities_to_keep: Sequence[Entity],
            partial: bool,
    ):
        conn = self._get_conn(config)
        project = config.project

        for table in tables_to_keep:
            columns = [f"{f.name} {self._datatype(f.dtype)}" for f in table.schema]

            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {_table_id(project, table)} ({', '.join(columns)}, event_ts timestamp, created_ts timestamp,  PRIMARY KEY({', '.join(table.entities)}))"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {_table_id(project, table)}_ek ON {_table_id(project, table)} ({', '.join(table.entities)});"
            )

        for table in tables_to_delete:
            conn.execute(f"DROP TABLE IF EXISTS {_table_id(project, table)}")

    def _datatype(self, dtype: FeastType):
        dt = dtype.to_value_type()
        if dt not in self._VALUE_TYPES_TO_SQL_TYPES:
            return self._VALUE_TYPES_TO_SQL_TYPES[ValueType.UNKNOWN]
        return self._VALUE_TYPES_TO_SQL_TYPES[dt]

    @log_exceptions_and_usage(online_store="vector")
    def plan(
            self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        project = config.project

        infra_objects: List[InfraObject] = [
            SqliteTable(
                path=self._get_db_path(config),
                name=_table_id(project, FeatureView.from_proto(view)),
            )
            for view in [
                *desired_registry_proto.feature_views,
                *desired_registry_proto.stream_feature_views,
            ]
        ]
        return infra_objects

    def teardown(
            self,
            config: RepoConfig,
            tables: Sequence[FeatureView],
            entities: Sequence[Entity],
    ):
        try:
            os.unlink(self._get_db_path(config))
        except FileNotFoundError:
            pass


def _initialize_conn(db_path: str):
    Path(db_path).parent.mkdir(exist_ok=True)
    return sqlite3.connect(
        db_path,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        check_same_thread=False,
    )


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


class SqliteTable(InfraObject):
    """
    A Sqlite table managed by Feast.

    Attributes:
        path: The absolute path of the Sqlite file.
        name: The name of the table.
        conn: SQLite connection.
    """

    path: str
    conn: sqlite3.Connection

    def __init__(self, path: str, name: str):
        super().__init__(name)
        self.path = path
        self.conn = _initialize_conn(path)

    def to_infra_object_proto(self) -> InfraObjectProto:
        sqlite_table_proto = self.to_proto()
        return InfraObjectProto(
            infra_object_class_type=SQLITE_INFRA_OBJECT_CLASS_TYPE,
            sqlite_table=sqlite_table_proto,
        )

    def to_proto(self) -> Any:
        sqlite_table_proto = SqliteTableProto()
        sqlite_table_proto.path = self.path
        sqlite_table_proto.name = self.name
        return sqlite_table_proto

    @staticmethod
    def from_infra_object_proto(infra_object_proto: InfraObjectProto) -> Any:
        return SqliteTable(
            path=infra_object_proto.sqlite_table.path,
            name=infra_object_proto.sqlite_table.name,
        )

    @staticmethod
    def from_proto(sqlite_table_proto: SqliteTableProto) -> Any:
        return SqliteTable(
            path=sqlite_table_proto.path,
            name=sqlite_table_proto.name,
        )

    def update(self):
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} (entity_key BLOB, feature_name TEXT, value BLOB, event_ts timestamp, created_ts timestamp,  PRIMARY KEY(entity_key, feature_name))"
        )
        self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self.name}_ek ON {self.name} (entity_key);"
        )

    def teardown(self):
        self.conn.execute(f"DROP TABLE IF EXISTS {self.name}")
