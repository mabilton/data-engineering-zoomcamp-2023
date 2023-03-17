import time
from math import inf
from typing import Sequence, Union

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


def consume_taxi_data(
    topic: Union[str, Sequence[str]],
    key_schema: str,
    value_schema: str,
    registry_url: str,
    bootstrap_servers: str,
    group_id: str,
    offset: str = "earliest",
    max_messages: int = inf,
    sleep: float = 0.0,
    verbose: bool = True,
) -> None:
    consumer = TaxiConsumer(
        key_schema, value_schema, registry_url, bootstrap_servers, group_id, offset
    )
    consumer.consume(topic, max_messages, sleep, verbose)
    return None


class TaxiConsumer:
    def __init__(
        self,
        key_schema: str,
        value_schema: str,
        registry_url: str,
        bootstrap_servers: str,
        group_id: str,
        offset: str = "earliest",
    ):
        self.key_deserializer = self.create_deserializer(key_schema, registry_url)
        self.value_deserializer = self.create_deserializer(value_schema, registry_url)
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": offset,
            }
        )

    @staticmethod
    def create_deserializer(schema: str, registry_url: str) -> AvroDeserializer:
        schema_registry_client = SchemaRegistryClient({"url": registry_url})
        with open(schema) as f:
            schema_str = f.read()
        return AvroDeserializer(schema_registry_client, schema_str)

    def consume(
        self,
        topics: Union[str, Sequence[str]],
        max_messages: int = inf,
        sleep: float = 0.0,
        verbose: bool = False,
    ) -> None:
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
                if num_consumed >= max_messages:
                    break
            except KeyboardInterrupt:
                break
        print(f"Succesfully consumed {num_consumed} records from topics: {topics}.")
        self.consumer.close()
        return None
