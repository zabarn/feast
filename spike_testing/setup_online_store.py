from datetime import datetime, timedelta, timezone
from feast import Field, Entity, FileSource, FeatureView, FeatureStore, ValueType
import os

file_path = f"{os.getcwd()}/source/data.parquet"
id_entity = Entity(name="rid", description="id", value_type=ValueType.INT64)
test_data_source = FileSource(name="testing_data_source", path=file_path, timestamp_field="timestamp")
test_feature_view = FeatureView(name="testing", entities=[id_entity], ttl=timedelta(days=1), source=test_data_source)

store = FeatureStore()
store.apply([id_entity, test_data_source, test_feature_view])
store.materialize(datetime.now(timezone.utc) - timedelta(days=1), datetime.now(timezone.utc), feature_views=["testing"])
