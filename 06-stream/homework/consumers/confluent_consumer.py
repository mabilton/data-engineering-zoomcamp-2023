import time
from math import inf
from typing import Literal, Sequence, Union

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

"""
Module containing a Kafka consumer that reads Green taxi and FHV taxi messages 
that have been produced to a Kafka topic by a `ConfluentTaxiProducer` from the 
`producers/confluent_producer.py` module.
"""


def consume_taxi_data(
    topic: Union[str, Sequence[str]],
    key_schema: str,
    value_schema: str,
    schema_registry_url: str,
    bootstrap_servers: str,
    group_id: str,
    offset: Literal["earliest", "latest"] = "earliest",
    min_messages: int = inf,
    sleep: float = 0.0,
    verbose: bool = True,
) -> None:
    """
    Consumes taxi data from a specified Kafka topic, assuming the messages in that topic
    were written by by a `ConfluentConfluentTaxiProducer` from the
    `producers/confluent_producer.py` module.

    This is achieved by first created a `ConfluentTaxiConsumer` object, and then calling
    it's `consume` method.

    Arguments
    ---------
    topic
        Name of the topic to consume taxi messages from.
    key_schema
        Path to Apache Arvo file that defines the schema of the key for each consumed
        message.
    value_schema
        Path to Apache Arvo file that defines the schema of the value for each consumed
        message.
    schema_registry_url
        URL of the schema registry; for more information on what a schema registry is and how
        it's used by Kafka, please refer to the following article:
        https://medium.com/slalom-technology/introduction-to-schema-registry-in-kafka-915ccf06b902
    bootstrap_servers
        URL to the bootstrap server(s) of the Kafka Cluster we wish to connect to.
    group_id
        Consumer group ID that created Kafka consumer should be added to.
    offset
        Specifies if consumer should start reading topic from `'earliest'` message,
        or start reading topic from `'latest'` message.
    min_messages
        The minimum number of messages that should be read from `topic`; the consumer
        will keep reading messages until this minimum number is met.
    sleep
        Number of seconds to wait after consuming a message before consuming the next message.
    verbose
        Whether the progress of the Kafka consumer should be printed to the user.
    """
    consumer = ConfluentTaxiConsumer(
        key_schema,
        value_schema,
        schema_registry_url,
        bootstrap_servers,
        group_id,
        offset,
    )
    consumer.consume(topic, min_messages, sleep, verbose)
    return None


class ConfluentTaxiConsumer:

    """
    Kafka consumer implemented using the `confluent` package that can read Green taxi
    or FHV taxi CSV data from a specified Kafka topic(s).

    This consumer deserializes the keys and values of consumed messages by using `confluent`'s
    Arvo deserializer. This means that if a read message was not produced using a
    `ConfluentTaxiProducer` object from the `producers/confluent_producer.py` module, the
    read message will be deserialized incorrectly.

    Attributes
    ----------
    consumer
        `KafkaConsumer` object (from the `confluent` library) that's used to consume
        messages from a Kafka topic(s).
    key_deserializer
        Arvo deserializer used to convert the key of a consumed message from bytes back
        into a Python dictionary.
    value_deserializer
        Arvo deserializer used to convert the value of a consumed message from bytes back
        into a Python dictionary.

    Methods
    -------
    consume
        Reads messages from a specified Kafka topic(s).
    create_deserializer
        Creates an Arvo deserializer from a path to an Apache Arvo file.
    """

    def __init__(
        self,
        key_schema: str,
        value_schema: str,
        schema_registry_url: str,
        bootstrap_servers: str,
        group_id: str,
        offset: Literal["earliest", "latest"] = "earliest",
    ):
        """
        Initializes `ConfluentTaxiConsumer` object.

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
        group_id
            Consumer group ID that created Kafka consumer should be added to.
        offset
            Specifies if consumer should start reading topic from `'earliest'` message,
            or start reading topic from `'latest'` message.
        """
        self.key_deserializer = self.create_deserializer(
            key_schema, schema_registry_url
        )
        self.value_deserializer = self.create_deserializer(
            value_schema, schema_registry_url
        )
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": offset,
            }
        )

    @staticmethod
    def create_deserializer(
        arvo_path: str, schema_registry_url: str
    ) -> AvroDeserializer:
        """
        Creates an Arvo deserializer from a path to an Apache Arvo file and the
        URL of a schema registry.
        """
        schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
        with open(arvo_path) as f:
            schema_str = f.read()
        return AvroDeserializer(schema_registry_client, schema_str)

    def consume(
        self,
        topics: Union[str, Sequence[str]],
        min_messages: int = inf,
        sleep: float = 0.0,
        verbose: bool = False,
    ) -> None:
        """
        Consumes taxi data messages from a specified list of topic(s).

        It is assumed that the messages that are being read from each topic were produced using
        a `ConfluentTaxiProducer` from the `producers/confluent_producer.py` module; if this
        isn't true, then the message key and value will be incorrectly deserialized.

        Arguments
        ---------
        topics
            Topic or list of topics to consume messages from.
        min_messages
            The minimum number of messages that should be read from `topic`; the consumer
            will keep reading messages until this minimum number is met.
        sleep
            Number of seconds to wait after consuming a message before consuming the next message.
        verbose
            Whether the progress of the Kafka consumer should be printed to the user.
        """
        if isinstance(topics, str):
            topics = [topics]
        self.consumer.subscribe(topics=topics)
        num_consumed = 0
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is not None:
                    key = self.key_deserializer(
                        msg.key(), SerializationContext(msg.topic(), MessageField.KEY)
                    )
                    value = self.value_deserializer(
                        msg.value(),
                        SerializationContext(msg.topic(), MessageField.VALUE),
                    )
                    if verbose:
                        print(f"Record {num_consumed + 1}, Key: {key}, Values: {value}")
                    time.sleep(sleep)
                    num_consumed += 1
                if num_consumed >= min_messages:
                    break
            except KeyboardInterrupt:
                break
        print(f"Succesfully consumed {num_consumed} records from topics: {topics}.")
        self.consumer.close()
        return None
