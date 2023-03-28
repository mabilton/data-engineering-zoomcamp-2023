from typing import Mapping, Optional, Sequence

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from utils import load_kafka_settings

"""
Module that contains functions to read, write, and transform PySpark Streaming Dataframes. 
"""


def read_df_from_kafka(
    spark: SparkSession, topic: str, kafka_settings: Optional[Mapping[str, str]] = None
) -> PySparkDataFrame:
    """
    Reads specified `topic` from the Kafka cluster defined by `kafka_settings`,
    and returns a PySpark streaming dataframe of the messages in that topic.
    """
    if kafka_settings is None:
        kafka_settings = load_kafka_settings()
    loaded_df = (
        spark.readStream.format("kafka")
        .option("client.dns.lookup", "use_all_dns_ips")
        .option(
            "kafka.bootstrap.servers",
            f"{kafka_settings['bootstrap_servers']},{kafka_settings['broker']}",
        )
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("checkpointLocation", "checkpoint")
        .load()
    )
    return loaded_df


def streaming_df_to_batch_df(
    streaming_df: PySparkDataFrame, spark: SparkSession, query_name: str = "query"
) -> PySparkDataFrame:
    """
    Converts a streaming dataframe to a regular 'batch' dataframe.
    """
    query = (
        streaming_df.writeStream.queryName(query_name)
        .format("memory")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    query_results = spark.sql(f"SELECT * FROM {query_name};")
    return query_results


def show_streaming_df(streaming_df: PySparkDataFrame, spark: SparkSession) -> None:
    """
    Prints contents of streaming dataframe to user.
    """
    streaming_df_to_batch_df(streaming_df, spark).show()
    return None


def parse_taxi_messages(df: PySparkDataFrame, schema: StructType) -> PySparkDataFrame:
    """
    Parses the value of each message read from a Kafka topic.
    """
    assert df.isStreaming, "DataFrame doesn't receive streaming data"
    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    # split attributes to nested array in one Column
    col = F.split(df["value"], ", ")
    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def count_streaming_df_rows(streaming_df: PySparkDataFrame, spark: SparkSession) -> int:
    """
    Counts number of rows in a streaming dataframe.
    """
    return streaming_df_to_batch_df(streaming_df, spark).count()


def prepare_df_for_producing(
    df: PySparkDataFrame, value_columns: Sequence[str], key_column: Optional[str] = None
) -> PySparkDataFrame:
    """
    Creates a streaming dataframe with a message 'key' and message 'value' column; the returned
    streaming dataframe is in a form that is appropriate to being written to a Kafka topic
    by calling the `write_df_to_topic` function.

    The 'key' column is formed by casting a specified `key_column` to a string.
    Conversely, the 'value' column is formed by casting all of the specified `value_columns`
    to strings, and then joining these strings together with commas. This strategy is identical
    to what's employed by the `KafkaPythonTaxiProducer` in the `producers/kafka_python_producer.py`
    module.

    Arguments
    ---------
    df
        Streaming dataframe that is to be prepared for writing to a Kafka topic
        by the `write_df_to_topic` function.
    value_columns
        List of column names that should be used as the value(s) of the Kafka messages to
        be written.
    key_column
        Column name to be used as keys for the Kafka messages to be written.

    Returns
    -------
    prepared_df
        Streaming dataframe with a 'key' and 'value' column, which contain the key and
        values of the messages to be written to Kafka respectively. The contents
        of this dataframe can then be written to a Kafka topic by passing it to the
        `write_df_to_topic` function.
    """

    df = df.withColumn("value", F.concat_ws(", ", *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", df["key"].cast("string"))
    else:
        df = df.withColumn("key", F.lit(None).cast("string"))
    return df.select(["key", "value"])


def write_df_to_topic(
    df: PySparkDataFrame,
    topic: str,
    kafka_settings: Optional[Mapping[str, str]] = None,
    write_freq: int = 5,
) -> StreamingQuery:
    """
    Writes a streaming dataframe to a topic within a specified Kafka cluster.

    Upon initialization of this write query, all of the messages already in
    `df` will be automatically written to `topic`. Any further messages that
    are streamed to `df` will also be automatically appended to `topic`.

    It is assumed that the provided `df` has been preprocessed by the
    `prepare_df_for_producing` function.

    Arguments
    ---------
    df
        Streaming dataframe whose contents should be written to Kafka.
        This dataframe should have been preprocessed by
        `prepare_df_for_producing`.
    topic
        Topic that `df`'s contents should be written to.
    kafka_settings
        Dictionary specifying configuration settings of Kafka Cluster that
        should be written to; see `kafka_settings.yml` for what should be
        contained in this dictionary.
    write_freq
        Number of seconds PySpark should wait before checking whether or
        not any new data has been streamed to `df` since the last time
        data was written to `topic`. If more data *has* been streamed
        since the last write operation, this new data is automatically
        appended to the `topic`.

    Returns
    -------
    write_query
        PySpark streaming query that automatically writes messages in `df`
        to specified `topic`.
    """

    if not df.isStreaming:
        raise ValueError("Only streaming DataFrames can be written to a Kafka topic.")
    if not all(col in df.columns for col in ["key", "value"]):
        raise ValueError(
            "Dataframe must contain a 'key' and a 'value' column; "
            "please preprocess your dataframe using `prepare_df_for_kafka` "
            "before attempting to write it to a Kafka topic."
        )
    if kafka_settings is None:
        kafka_settings = load_kafka_settings()
    write_query = (
        df.writeStream.trigger(processingTime=f"{write_freq} seconds")
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            f"{kafka_settings['bootstrap_servers']},{kafka_settings['broker']}",
        )
        .outputMode("append")
        .option("topic", topic)
        .option("checkpointLocation", "checkpoint")
        .start()
    )
    return write_query
