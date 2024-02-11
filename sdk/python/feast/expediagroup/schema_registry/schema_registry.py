"""
Wrapper for SchemaRegistryClient, to separate Feast from
the extensive auth and configuration process of
connecting to a SchemaRegistry.

Copyright 2024 Expedia Group
Author: matcarlin@expediagroup.com
"""

import requests

from confluent_kafka.schema_registry import SchemaRegistryClient


class SchemaRegistry():
    # spark: SparkSession
    # format: str
    # preprocess_fn: Optional[MethodType]
    # join_keys: List[str]

    def __init__(self):
        pass

    def get_properties(
    	user: String,
    	password: String,
    	urn: String,
    	environment: String,
    	cert_path: String, #https://stackoverflow.com/questions/55203791/python-requests-using-certificate-value-instead-of-path
    ) -> dict:
    """Discover a Schema Registry with the provided urn and credentials,
    and obtain a set of properties for use in Schema Registry calls."""
    	discovery_url = "https://stream-discovery-service-{environment}.rcp.us-east-1.data.{environment}.exp-aws.net/v2/discovery/urn/{urn}".format(
	        environment=environment, urn=urn
	    )

	    response = requests.get(
	        discovery_url,
	        auth=(user, password),
	        headers={"Accept": "application/json"},
	        verify=cert_path,
	    )

	    if response.status_code != 200:
	        raise RuntimeError(
	            "Discovery API returned unexpected HTTP status: {status}".format(
	                status=str(response.status_code)
	            )
	        )

	    try:
	        props = json.loads(response.text)
	    except (TypeError, UnicodeDecodeError):
	        raise TypeError(
	            "Discovery API response did not contain valid json: {response}".format(
	                response=response.text
	            )
	        )

	    return props