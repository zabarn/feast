# Creating an OnlineStore
* Add a file in `sdk/python/feast/infra/online_store/contrib/<name>.py`
* Add an entry in the `ONLINE_STORE_CLASS_FOR_TYPE` dict in `sdk/python/feast/repo_config.py` so feast can pick up the new online store
* Create a `<name>OnlineStoreConfig` class in contrib file. This defines the parameters for the `feature_store.yaml` configuration
* Create a `<name>OnlineStore` class with db-specific functionality 

By default Feast wants to insert all features in one row per feature per entity key with everything encoded in as binary strings. This is not compatible with vector searches or for easy direct connect. However, most of that transformation is done in the OnlineStore methods, so we can ignore most of it. However, we will need to do some mapping for responses as Protos. Generic utils can be created for this within the vector module.

## Feast Data Structure

Assume the below JSON is a single row of a feature set input with `id` as the entity_key in the FeatureView.
```json
{
    "id": 1,
    "name": "abc",
    "data": 45.6,
    "number": 4
}
```

The standard Feast pattern is to save the features in the online store like this:

| entity_key | feature_name | value   | event_ts            | created_ts          |
|-----------| ------------ | ------- | ------------------- | ------------------- |
| b'1+name' | b'name'      | b'abc'  | 2023-01-10 00:00:00 | 2023-01-10 04:13:27 |
| b'1+data' | b'data'      | b'45.6' | 2023-01-10 00:00:00 | 2023-01-10 04:13:27 |
| b'1+number' | b'number'    | b'4'    | 2023-01-10 00:00:00 | 2023-01-10 04:13:27 |

All data values are stored as binary strings with one row per feature per entity_key.

## Get Features

Online read does a bunch of validations that make overriding it to use a similarity search extremely difficult and hacky. First, it validates that the input entity keys match how they are defined in the FeatureView exactly. So if a FeatureView has an entity key of `{id: int64, vec: List[float]}` it expects a list of `{id, vec}`'s with those value types. It then expects (and validates) the response to be a Dict with keys of each of the requested features and the entity key with the values an ordered list of the associated values. These validations aren't compatible with how similarity search should work.

example response structure:
```python
{
    "id": [ 1, 2, 3, ... ],
    "name": [ 'abc', 'def', 'ghi', ... ],
    "data": [ 45.6, 32.1, 17.7, ... ],
    "number": [ 4, 7, 12, ... ]
}
```

## Spike Example
Example included uses `sqlite` as a base for ease of hacking. Hacked OnlineStore is: `sdk/python/feast/infra/online_store/contrib/vector.py`

Run `./source/random_data.py` to generate random data.

Run `setup_online_Store.py` to populate store with generated parquet file.

Run `testing_get.py` to make a read request.
