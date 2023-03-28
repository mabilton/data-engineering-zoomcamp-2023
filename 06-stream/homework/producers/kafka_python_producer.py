import csv
import json
import time
from math import inf
from typing import Optional, Union

from kafka import KafkaProducer

"""
Module containing a Kafka producer that writes messages from Green taxi 
and FHV taxi CSV files to Kafka; this producer is implemented using the 
`kafka-python` package.
"""


def produce_taxi_data(
    csv_path: str,
    topic: str,
    key_schema: str,
    value_schema: str,
    bootstrap_servers: str,
    max_messages: int = inf,
    sleep: float = 0.0,
    verbose: bool = False,
    start_row: int = 0,
    schema_registry_url: Optional[str] = None,
) -> None:
    """
    Produces data from a taxi data CSV file to a specified topic.

    This is done by first initializing a `KafkaPythonTaxiProducer` object, and then calling
    its `produce_csv` method.

    Arguments
    ---------
    csv_path
        Path to taxi data CSV; the rows of this CSV will be written to a specified Kafka topic.
    topic
        Name of the topic to produce messages to.
    key_schema
        Path to Apache Arvo file that defines the schema of the key for each produced message.
    value_schema
        Path to Apache Arvo file that defines the schema of the value for each produced message.
    bootstrap_servers
        URL to the bootstrap server(s) of the Kafka Cluster we wish to connect to.
    max_messages
        The maximum number of messages that should be produced to `topic`; if `max_messages`
        exceeds the number of rows in the CSV file, then the producer will only produce as
        many messages as there are rows in the CSV.
    sleep
        Number of seconds to wait after producing a message before producing the next message.
    verbose
        Whether the progress of the Kafka producer should be printed to the user.
    start_row
        Number of row in CSV to start producing data from. For example, `start_row = 5` would
        mean only the `5`th row and onwards would be produced to the Kafka cluster.
    schema_registry_url
        URL of the schema registry. Although this input is unused, it is accepted so that
        this function's call signature matches the `produce_taxi_data` function of the
        `confluent_producer.py` module.
    """
    producer = KafkaPythonTaxiProducer(key_schema, value_schema, bootstrap_servers)
    producer.produce_csv(csv_path, topic, max_messages, sleep, start_row, verbose)
    return None


class KafkaPythonTaxiProducer:

    """
    Kafka producer implemented using the `kafka-python` package that can write Green taxi
    or FHV taxi CSV data to any topic within a specific Kafka Cluster.

    This producer serializes the keys and values of produced messages by using first joining
    all the keys/values together into a single comma-separated string, and then serializing
    this string.

    Attributes
    ----------
    producer
        `KafkaProducer` object (from the `kafka-python` library) that's used to write
        messages to the Kafka Cluster.
    key_schema_dict
        Dictionary specifying the type of the CSV column(s) that are to used as message keys.
    value_schema_dict
        Dictionary specifying the type of the CSV column(s) that are to used as message values.

    Methods
    -------
    produce_csv
        Writes the rows of a specified CSV as messages to a specified topic.
    produce_single_message
        Writes a key-value pair as a single message to a specified Kafka topic.
    serializer
        Static method that serializes the message keys and values the into bytes.
    load_avro_schema_as_dict
        Static method that loads the schema specified by an Apache Arvo file as
        a Python dictionary.
    """

    def __init__(
        self,
        key_schema: str,
        value_schema: str,
        bootstrap_servers: Union[str, list[str]],
    ):
        """
        Initializes `KafkaPythonTaxiProducer` object.

        Arguments
        ---------
        key_schema
            Path to Apache Arvo file that specifies schema for message keys.
        value_schema
            Path to Apache Arvo file that specifies schema for message keys.
        bootstrap_servers
            URL or list of URLS of bootstrap servers associated with the Kafka cluster
            you want to write messages to. This input is used to specify *which* Kafka
            cluster to write to.
        """
        if isinstance(bootstrap_servers, str):
            bootstrap_servers = [bootstrap_servers]
        self.key_schema_dict = self.load_avro_schema_as_dict(key_schema)
        self.value_schema_dict = self.load_avro_schema_as_dict(value_schema)
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=self.serializer,
            value_serializer=self.serializer,
        )

    @staticmethod
    def serializer(x: list[str]) -> bytes:
        """
        Takes list of strings (i.e. the message key or message values
        we want to write to Kafka), joins them together with commas,
        then encodes string as bytes.
        """
        return ", ".join(x).encode("utf-8")

    @staticmethod
    def load_avro_schema_as_dict(avro_path: str) -> dict[str, list[str]]:
        """
        Loads Apache Arvo files located at `arvo_path`, and returns the
        `'fields'` section of this file as a dictionary.
        """
        with open(avro_path) as f:
            avro_schema = json.load(f)
        schema_dict = {}
        for field in avro_schema["fields"]:
            types = field["type"]
            if not isinstance(types, list):
                types = [types]
            schema_dict[field["name"]] = types
        return schema_dict

    def produce_csv(
        self,
        csv_path: str,
        topic: str,
        max_messages: int = inf,
        sleep: float = 0.0,
        start_row: int = 0,
        verbose: bool = False,
    ) -> None:
        """
        Produces data from a taxi data CSV file to a specified topic.

        Arguments
        ---------
        csv_path
            Path to taxi data CSV; the rows of this CSV will be written to a specified Kafka topic.
        topic
            Name of the topic to produce messages to.
        max_messages
            The maximum number of messages that should be produced to `topic`; if `max_messages`
            exceeds the number of rows in the CSV file, then the producer will only produce as
            many messages as there are rows in the CSV.
        sleep
            Number of seconds to wait after producing a message before producing the next message.
        start_row
            Number of row in CSV to start producing data from. For example, `start_row = 5` would
            mean only the `5`th row and onwards would be produced to the Kafka cluster.
        verbose
            Whether the progress of the Kafka producer should be printed to the user.
        """
        num_produced = 0
        skipped_rows = 0
        with open(csv_path) as f:
            reader = csv.reader(f)
            col_names = next(reader)
            key_idxs = {
                key: col_names.index(key) for key in self.key_schema_dict.keys()
            }
            vals_idxs = {
                val: col_names.index(val) for val in self.value_schema_dict.keys()
            }
            for line in reader:
                if skipped_rows < start_row:
                    skipped_rows += 1
                    continue
                else:
                    key_i = [str(line[idx]) for idx in key_idxs.values()]
                    value_i = [str(line[idx]) for idx in vals_idxs.values()]
                    self.produce_single_message(topic, key_i, value_i, verbose)
                    time.sleep(sleep)
                    num_produced += 1
                    if num_produced >= max_messages:
                        break
        print(f"Successfully produced {num_produced} messages to '{topic}' topic.")
        return None

    def produce_single_message(
        self,
        topic: str,
        key: list[str],
        value: list[str],
        verbose: bool = False,
    ) -> None:
        """
        Produces a single message (which corresponds to the data stored in a single row
        within the Taxi CSV) to a specified Kafka topic.

        Arguments
        ---------
        topic
            Name of topic to produce single message to.
        key
            List of strings to be used as the key(s) of the message.
        value
            List of strings to be used as the values of the message.
        verbose
            Whether metadata of produced message should be printed to the user.
        """
        try:
            future_metadata = self.producer.send(topic=topic, key=key, value=value)
            metadata = future_metadata.get()
            if verbose:
                print(
                    f"Record successfully produced to "
                    f"Partition {metadata.partition} of topic '{metadata.topic}' "
                    f"at offset {metadata.offset}."
                )
        except Exception as err:
            if verbose:
                print(f"Delivery failed for record {key}: {err}.")
            raise err
        self.producer.flush()
        return None
