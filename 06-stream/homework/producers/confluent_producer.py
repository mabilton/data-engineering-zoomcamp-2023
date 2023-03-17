import csv
import json
import time
from math import inf
from typing import Any, Mapping

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


def produce_taxi_data(
    csv_path: str,
    topic: str,
    key_schema: str,
    value_schema: str,
    registry_url: str,
    bootstrap_servers: str,
    max_messages: int = inf,
    sleep: float = 0.0,
    skip_rows: int = 0,
    verbose: bool = False,
) -> None:
    producer = TaxiProducer(key_schema, value_schema, registry_url, bootstrap_servers)
    producer.produce_csv(csv_path, topic, sleep, max_messages, skip_rows, verbose)
    return None


class TaxiProducer:
    def __init__(
        self,
        key_schema: str,
        value_schema: str,
        registry_url: str,
        bootstrap_servers: str,
    ):
        self.key_schema_dict = self.load_avro_schema_as_dict(key_schema)
        self.key_serializer = self.create_serializer(key_schema, registry_url)
        self.value_schema_dict = self.load_avro_schema_as_dict(value_schema)
        self.value_serializer = self.create_serializer(value_schema, registry_url)
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})

    @staticmethod
    def load_avro_schema_as_dict(avro_path: str) -> dict[str, list[str]]:
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
    def create_serializer(schema: str, registry_url: str) -> AvroSerializer:
        schema_registry_client = SchemaRegistryClient({"url": registry_url})
        with open(schema) as f:
            schema_str = f.read()
        return AvroSerializer(schema_registry_client, schema_str)

    def produce_csv(
        self,
        csv_path: str,
        topic: str,
        sleep: float = 0.0,
        max_messages: int = inf,
        skip_rows: int = 0,
        verbose: bool = False,
    ) -> None:
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
                if skipped_rows < skip_rows:
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
        elif value_str == "" and ("null" in dtypes):
            value = None
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
    def delivery_report(err, msg) -> None:
        if err is not None:
            print(f"Delivery failed for record {msg.key()}: {err}.")
        else:
            print(
                f"Record successfully produced to "
                f"Partition {msg.partition()} of topic '{msg.topic()}' "
                f"at offset {msg.offset()}."
            )
        return None
