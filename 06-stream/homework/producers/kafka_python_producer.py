import csv
import json
import time
from math import inf
from typing import Any, Mapping, Union

from kafka import KafkaProducer


def produce_taxi_data(
    csv_path: str,
    topic: str,
    key_schema: str,
    value_schema: str,
    bootstrap_servers: str,
    max_messages: int = inf,
    sleep: float = 0.0,
    verbose: bool = False,
    skip_rows: int = 0,
    **kwargs,
) -> None:
    producer = TaxiProducer(key_schema, value_schema, bootstrap_servers)
    producer.produce_csv(csv_path, topic, sleep, max_messages, skip_rows, verbose)
    return None


class TaxiProducer:
    def __init__(
        self,
        key_schema: str,
        value_schema: str,
        bootstrap_servers: Union[str, list[str]],
    ):
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
    def serializer(x: list[str]):
        return ", ".join(x).encode("utf-8")

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

    def produce_csv(
        self,
        csv_path: str,
        topic: str,
        sleep: float = 0.0,
        max_messages: int = inf,
        skip_rows: int = 0,
        verbose: bool = False,
    ) -> None:
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
                if skipped_rows < skip_rows:
                    skipped_rows += 1
                    continue
                else:
                    key_i = [str(line[idx]) for idx in key_idxs.values()]
                    value_i = [str(line[idx]) for idx in vals_idxs.values()]
                    self.produce_single_record(topic, key_i, value_i, verbose)
                    time.sleep(sleep)
                    num_produced += 1
                    if num_produced >= max_messages:
                        break
        print(f"Successfully produced {num_produced} messages to '{topic}' topic.")
        return None

    def produce_single_record(
        self,
        topic: str,
        key: Mapping[str, Any],
        value: Mapping[str, Any],
        verbose: bool = False,
    ) -> None:
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
