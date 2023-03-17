import time
from math import inf
from typing import Sequence, Union

from kafka import KafkaConsumer


def consume_taxi_data(
    topic: Union[str, Sequence[str]],
    bootstrap_servers: str,
    group_id: str,
    offset: str = "earliest",
    max_messages: int = inf,
    sleep: float = 0.0,
    verbose: bool = False,
    **kwargs,
) -> None:
    consumer = TaxiConsumer(bootstrap_servers, group_id, offset)
    consumer.consume(topic, max_messages, sleep, verbose)
    return None


class TaxiConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, offset: str = "earliest"):
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
        return x.decode("utf-8").split(", ")

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
                        if num_consumed >= max_messages:
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
