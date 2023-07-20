# Creating an OnlineStore
* Add a file in `sdk/python/feast/infra/online_store/contrib/<name>.py`
* Add an entry in the `ONLINE_STORE_CLASS_FOR_TYPE` dict in `sdk/python/feast/repo_config.py` so feast can pick up the new online store
* Create a `<name>OnlineStoreConfig` class in contrib file. This defines the parameters for the `feature_store.yaml` configuration
* Create a `<name>OnlineStore` class with db-specific functionality 

By default Feast wants to insert all features in one row per feature per entity key with everything encoded in as binary strings. This is not compatible with vector searches or for easy direct connect. However, most of that transformation is done in thge OnlineStore methods so we can ignore most of it. However, we will need to do some mapping for responses as Protos. Generic utils can be created for this within the vector module.

Online read does a bunch of validations that make overriding it to use a similarity search extremely difficult and hacky. First, it validates that the input entity keys match how they are defined in the FeatureView exactly. So if a FeatureView has an entity key of `{id: int64, vec: List[float]}` it expects a list of `{id, vec}`'s with those value types. It then expects (and validates) the response to be a Dict with keys of each of the requested features and the entity key with the values an ordered list of the associated values. This isn't really compatible with how similarity search should work and respond.

## Spike Example
Example included uses `sqlite` as a base for ease of hacking. Hacked OnlineStore is: `sdk/python/feast/infra/online_store/contrib/vector.py`

Run `./source/random_data.py` to generate random data.

Run `setup_online_Store.py` to populate store with generated parquet file.

Run `testing_get.py` to make a read request.
