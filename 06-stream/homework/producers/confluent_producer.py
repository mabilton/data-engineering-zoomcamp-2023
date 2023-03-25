import csv
import json
import time
from math import inf
from typing import Any, Mapping, Optional

from confluent_kafka import Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

"""
Module containing a Kafka producer that writes messages from Green taxi 
and FHV taxi CSV files to Kafka; this producer is implemented using the 
`confluent` package.
"""


def produce_taxi_data(
    csv_path: str,
    topic: str,
    key_schema: str,
    value_schema: str,
    schema_registry_url: str,
    bootstrap_servers: str,
    max_messages: int = inf,
    sleep: float = 0.0,
    start_row: int = 0,
    verbose: bool = False,
) -> None:
    """
    Produces data from a taxi data CSV file to a specified topic.

    This is done by first initializing a `ConfluentTaxiProducer` object, and then calling
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
    schema_registry_url
        URL of the schema registry; for more information on what a schema registry is and how
        it's used by Kafka, please refer to the following article:
        https://medium.com/slalom-technology/introduction-to-schema-registry-in-kafka-915ccf06b902
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
    """
    producer = ConfluentTaxiProducer(
        key_schema, value_schema, schema_registry_url, bootstrap_servers
    )
    producer.produce_csv(csv_path, topic, max_messages, sleep, start_row, verbose)
    return None


class ConfluentTaxiProducer:

    """
    Kafka producer implemented using the `confluent` package that can write Green taxi
    or FHV taxi CSV data to any topic within a specific Kafka Cluster.

    This producer serializes the keys and values of produced messages by using `confluent`'s
    Arvo serializer.

    Attributes
    ----------
    producer
        `KafkaProducer` object (from the `confluent` library) that's used to write
        messages to the Kafka Cluster.
    key_schema_dict
        Dictionary specifying the type of the CSV column(s) that are to used as message keys.
    key_serializer
         Arvo serializer used to serialize message keys into bytes.
    value_schema_dict
        Dictionary specifying the type of the CSV column(s) that are to used as message values.
    value_serializer
        Arvo serializer used to serialize message values into bytes.

    Methods
    -------
    produce_csv
        Writes the rows of a specified CSV as messages to a specified topic.
    produce_single_message
        Writes a key-value pair as a single message to a specified Kafka topic.
    create_serializer
        Static method that creates an Arvo serializer from an Apache Arvo file.
    load_avro_schema_as_dict
        Static method that loads the schema specified by an Apache Arvo file as
        a Python dictionary.
    cast_str_to_schema_dtype
        Static method that casts a string to a specified data type.
    delivery_report
        Static method that prints metadata of produced message to the user.
    """

    def __init__(
        self,
        key_schema: str,
        value_schema: str,
        schema_registry_url: str,
        bootstrap_servers: str,
    ):
        """
        Initializes `ConfluentTaxiProducer` object.

        Arguments
        ---------
        key_schema
            Path to Apache Arvo file that specifies schema for message keys.
        value_schema
            Path to Apache Arvo file that specifies schema for message keys.
        schema_registry_url
            URL to schema registry.
        bootstrap_servers
            URL or list of URLS of bootstrap servers associated with the Kafka cluster
            you want to write messages to. This input is used to specify *which* Kafka
            cluster to write to.
        """
        self.key_schema_dict = self.load_avro_schema_as_dict(key_schema)
        self.key_serializer = self.create_serializer(key_schema, schema_registry_url)
        self.value_schema_dict = self.load_avro_schema_as_dict(value_schema)
        self.value_serializer = self.create_serializer(
            value_schema, schema_registry_url
        )
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})

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

    @staticmethod
    def create_serializer(arvo_path: str, schema_registry_url: str) -> AvroSerializer:
        """
        Creates a `confluent` Arvo serializer from a path to an Apache Arvo file and the
        URL of a schema registry.
        """
        schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
        with open(arvo_path) as f:
            schema_str = f.read()
        return AvroSerializer(schema_registry_client, schema_str)

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
        skipped_rows = 0
        num_produced = 0
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
                    key_i = {
                        name: self.cast_str_to_schema_dtype(
                            line[idx], self.key_schema_dict[name], name
                        )
                        for name, idx in key_idxs.items()
                    }
                    value_i = {
                        name: self.cast_str_to_schema_dtype(
                            line[idx], self.value_schema_dict[name], name
                        )
                        for name, idx in vals_idxs.items()
                    }
                    self.produce_single_record(topic, key_i, value_i, verbose)
                    time.sleep(sleep)
                    num_produced += 1
                    if num_produced >= max_messages:
                        break
        print(f"Successfully produced {num_produced} messages to '{topic}' topic.")
        return None

    @staticmethod
    def cast_str_to_schema_dtype(
        value_str: str, dtypes: list[str], col_name: str
    ) -> Any:
        """
        Casts a string to a particular data type.

        Arguments
        ---------
        value_str
            String to cast to a particular data type.
        dtypes
            List of valid data types for `value_str`. `dtypes` is either:
                1. A single element list that specifies the type the string should
                   be cast to. For example, `dtypes = ['int']` would cast
                  `value_str` to an `int`.
                2. A two element list, where one of the elements is 'null`; this
                   indicates that `value_str` could be empty and, if so, should be
                   casted to `None`. For example, `dtypes = ['int', 'null']` would
                   cast `value_str` to an `int` if it's a non-empty string, and to
                   `None` is `value_str` is empty.
        col_name
            Name of column whose value is being casted; this name is used when printing
            errors to the user.
        """
        if value_str == "":
            if "null" in dtypes:
                value = None
            else:
                raise ValueError(
                    f"Avro schema specifies that {col_name} column has "
                    "non-null values, but an empty value was encountered."
                )
        elif "int" in dtypes:
            value = int(value_str)
        elif "float" in dtypes:
            value = float(value_str)
        elif "boolean" in dtypes:
            value = bool(value_str)
        elif "string" in dtypes:
            value = value_str
        else:
            raise ValueError(f"'{dtypes}' are not supported dtypes.")
        return value

    def produce_single_record(
        self,
        topic: str,
        key: Mapping[str, Any],
        value: Mapping[str, Any],
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
            self.producer.produce(
                topic=topic,
                key=self.key_serializer(
                    key, SerializationContext(topic=topic, field=MessageField.KEY)
                ),
                value=self.value_serializer(
                    value, SerializationContext(topic=topic, field=MessageField.VALUE)
                ),
                on_delivery=self.delivery_report if verbose else None,
            )
        except Exception as e:
            raise e
        self.producer.flush()
        return None

    @staticmethod
    def delivery_report(err: Optional[Exception], msg: Message) -> None:
        """
        Prints metadata of produced message to user.
        """
        if err is not None:
            print(f"Delivery failed for record {msg.key()}: {err}.")
        else:
            print(
                f"Record successfully produced to "
                f"Partition {msg.partition()} of topic '{msg.topic()}' "
                f"at offset {msg.offset()}."
            )
        return None
