import pyspark.sql.functions as F
from utils import load_kafka_settings


def load_df_from_kafka(spark, topic: str, kafka_settings=None):
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


def streaming_df_to_batch_df(streaming_df, spark, query_name="query"):
    query = (
        streaming_df.writeStream.queryName(query_name)
        .format("memory")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    query_results = spark.sql(f"SELECT * FROM {query_name};")
    return query_results


def show_streaming_df(streaming_df, spark):
    streaming_df_to_batch_df(streaming_df, spark).show()
    return None


def parse_taxi_messages(df, schema):
    """take a Spark Streaming df and parse value
    col based on <schema>, return streaming df cols in schema"""
    assert df.isStreaming, "DataFrame doesn't receive streaming data"
    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    # split attributes to nested array in one Column
    col = F.split(df["value"], ", ")
    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def streaming_df_row_count(streaming_df, spark) -> int:
    return streaming_df_to_batch_df(streaming_df, spark).count()


def prepare_df_for_producing(df, value_columns, key_column=None):
    df = df.withColumn("value", F.concat_ws(", ", *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", df["key"].cast("string"))
    else:
        df = df.withColumn("key", F.lit(None).cast("string"))
    return df.select(["key", "value"])


def write_batch_df_to_kafka(df, topic, kafka_settings=None):
    if df.isStreaming:
        raise ValueError(
            "`write_batch_df_to_kafka` can only write batch (i.e. non-streaming) "
            "dataframes to a Kafka topic; please use `write_streaming_df_to_kafka` "
            "instead."
        )
    _check_df_cols_before_kafka_writing(df)
    if kafka_settings is None:
        kafka_settings = load_kafka_settings()
    write_query = (
        df.write.format("kafka")
        .option("kafka.bootstrap.servers", kafka_settings["bootstrap_servers"])
        .option("topic", topic)
        .save()
    )
    return write_query


def write_streaming_df_to_kafka(df, topic, kafka_settings=None):
    if not df.isStreaming:
        raise ValueError(
            "`write_streaming_df_to_kafka` can only write streaming "
            "dataframes to a Kafka topic; please use `write_batch_df_to_kafka` "
            "instead."
        )
    _check_df_cols_before_kafka_writing(df)
    if kafka_settings is None:
        kafka_settings = load_kafka_settings()
    write_query = (
        df.writeStream.format("kafka")
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


def _check_df_cols_before_kafka_writing(df) -> None:
    if not all(col in df.columns for col in ["key", "value"]):
        raise ValueError(
            "Dataframe must contain a 'key' and a 'value' column; "
            "please preprocess your dataframe using `prepare_df_for_kafka` "
            "before attempting to write it to a Kafka topic."
        )
    return None
