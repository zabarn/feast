from feast import FeatureStore

store = FeatureStore()
entities = [{"rid": x} for x in range(12, 23)]
# entities = [{"vec": [0, 0, 0], "topk": 10}] <- does not work. get validates that the input are all entity ids that match schema
result = store.get_online_features(["testing:name", "testing:vec"], entity_rows=entities).to_dict()
print(result)
