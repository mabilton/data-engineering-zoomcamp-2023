import time
from math import inf
from typing import Literal, Optional, Sequence, Union

from kafka import KafkaConsumer

"""
Module containing a Kafka consumer that reads Green taxi and FHV taxi messages 
that have been produced to a Kafka topic by a `KafkaPythonTaxiProducer` from the 
`producers/kafka_python_producer.py` module.
"""


def consume_taxi_data(
    topic: Union[str, Sequence[str]],
    bootstrap_servers: str,
    group_id: str,
    offset: Literal["earliest", "latest"] = "earliest",
    min_messages: int = inf,
    sleep: float = 0.0,
    verbose: bool = False,
    schema_registry_url: Optional[str] = None,
    key_schema: Optional[str] = None,
    value_schema: Optional[str] = None,
) -> None:
    """
    Consumes taxi data from a specified Kafka topic, assuming the messages in that topic
    were written by by a `KafkaPythonTaxiProducer` from the `producers/python_kafka_producer.py`
    module.

    This is achieved by first created a `KafkaPythonTaxiConsumer` object, and then calling it's
    `consume` method.

    Arguments
    ---------
    topic
        Name of the topic to consume taxi messages from.
    key_schema
        Path to Apache Arvo file that defines the schema of the key for each consumed message.
    value_schema
        Path to Apache Arvo file that defines the schema of the value for each consumed message.
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
    schema_registry_url
        URL of the schema registry. Although this input is unused, it is accepted so that this
        function's call signature matches the `consume_taxi_data` function of the
        `confluent_consumer.py` module.
    key_schema
        Path to Apache Arvo file that defines the schema of the key for each consumed
        message. Although this input is unused, it is accepted so that this
        function's call signature matches the `consume_taxi_data` function of the
        `confluent_consumer.py` module.
    value_schema
        Path to Apache Arvo file that defines the schema of the value for each consumed
        message. Although this input is unused, it is accepted so that this
        function's call signature matches the `consume_taxi_data` function of the
        `confluent_consumer.py` module.
    """
    consumer = KafkaPythonTaxiConsumer(bootstrap_servers, group_id, offset)
    consumer.consume(topic, min_messages, sleep, verbose)
    return None


class KafkaPythonTaxiConsumer:

    """
    Kafka consumer implemented using the `kafkka-python` package that can read Green taxi
    or FHV taxi CSV data from a specified Kafka topic(s).

    This consumer deserializes the keys and values of consumed messages by first deserializing
    the key/value bytes into a string, and then splitting this string into comma-separated
    values. This means that if a read message was not produced using a
    `KafkaPythonTaxiProducer` object from the `producers/kafka_python_producer.py` module, the
    read message will be deserialized incorrectly.

    Attributes
    ----------
    consumer
        `KafkaConsumer` object (from the `kafka-python` library) that's used to consume
        messages from a Kafka topic(s).

    Methods
    -------
    consume
        Reads messages from a specified Kafka topic(s).
    deserializer
        Static method that deserializes key and values of read messages.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        offset: Literal["earliest", "latest"] = "earliest",
    ):
        """
        Initializes `KafkaPythonTaxiConsumer` object.

        Arguments
        ---------
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
        if isinstance(bootstrap_servers, str):
            bootstrap_servers = [bootstrap_servers]
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=offset,
            enable_auto_commit=True,
            key_deserializer=self.deserializer,
            value_deserializer=self.deserializer,
            group_id=group_id,
        )

    @staticmethod
    def deserializer(x):
        """
        Deserializes a message key or value, assuming that the message was
        produced using a `kafka-python` producer.
        """
        return x.decode("utf-8").split(", ")

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
        a `KafkaPythonTaxiProducer` from the `producers/kafka_python_producer.py` module.
        If this isn't true, then the message key and value will be incorrectly deserialized.

        Arguments
        ---------
        topics
            Topic or list of topics to consume messages from.
        min_messages
            The minimum number of messages to consume from the Kafka topic(s). The consumer
            will keep consuming until this minimum number of messages is consumed.
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
                if msg is None or msg == {}:
                    continue
                for values in msg.values():
                    for value in values:
                        if verbose:
                            print(
                                f"Record {num_consumed + 1}, "
                                f"Key: {value.key}, "
                                f"Values: {value.value}"
                            )
                        time.sleep(sleep)
                        num_consumed += 1
                        if num_consumed >= min_messages:
                            break
                    else:
                        continue
                    break
                else:
                    continue
                break
            except KeyboardInterrupt:
                break
        print(f"Succesfully consumed {num_consumed} records from topics: {topics}.")
        self.consumer.close()
        return None
