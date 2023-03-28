# Week 6 Homework Answers

This file contains my answers to the Week 6 homework questions for the 2023 cohort of the Data Engineering Zoomcamp. For convenience, each question is restated before giving the corresponding answer; the list of questions *without* corresponding answers can be found in `01-questions.md`, which can be found in the current directory.

Please note that all of the intructions provided here **assume that your terminal is located in the `06-stream/homework` directory**.

## Set-Up

Before going through these answers, you should install the requirements listed in `requirements.txt`:
```bash
pip install -r requirements.txt
```

## Section 1: Theoretical Questions

The first part of this homework consist of theoretical questions on Kafka and streaming concepts.

### Question 1

#### Question

Please select all of the statements which are correct:
- Kafka Nodes are responsible for storing topics
- Zookeeper is to be removed from Kafka 4.0 and onwards
- Message retention ensures that messages do not get lost over a specific period of time.
- Group IDs allow for messages to be consumed by multiple distributed consumers simultaneously

#### Answer

It turns out that **all** of the presented statements are **correct**; let's go over each one in turn:
- *Kafka Nodes are responsible for storing topics* - **True**. A Kafka cluster consists of individual nodes (also known as Brokers) that are connected to one another in a network. The nodes in this network are responsible for the distributed storage and retrieval of messages sent to the cluster; this means this first statement is true.
- *Zookeeper is to be removed from Kafka 4.0 and onwards* - **True**. Kafka uses [Apache Zookeeper](https://zookeeper.apache.org/) to help coordinate the behaviour of all of the nodes in the Kafka cluster. As Kafka has been developed, however, an alternative piece of service synchronisation software has been developed, called [KRaft](https://developer.confluent.io/learn/kraft/); the main reason behind replacing Zookeeper with KRaft is to improve the scalability of Kafka (see [here](https://www.confluent.io/blog/why-replace-zookeeper-with-kafka-raft-the-log-of-all-logs/) and [here](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum
) for further details). At this point in time, Apache plans to completely replace Zookeeper with KRaft from Kafka 4.0 and onwards, as explained [here](https://cwiki.apache.org/confluence/display/KAFKA/KIP-833%3A+Mark+KRaft+as+Production+Ready). 
- *Message retention ensures that messages do not get lost over a specific period of time.* - **True**. Message retention refers to how long the brokers in a Kafka cluster will store any given message before automatically deleting it to free up space for future messages. By setting the retention time to 5 hours, for instance, we ensure that any given message stored in our cluster does "not get lost" for 5 hours.
- *Group IDs allow for messages to be consumed by multiple distributed consumers simultaneously* - **True**. Kafka Consumers are able to ge grouped together within consumer groups; these consumer groups each have an associated group ID. Multiple consumers within the same consumer group can consume from different partitions of the same topic simultaneously, as explained [here](https://www.confluent.io/blog/configuring-apache-kafka-consumer-group-ids/).

### Question 2

#### Question

Which of the following Kafka features support reliability and availability?
- Topic Replication
- Topic Partitioning
- Consumer Group ID
- Ack All

#### Answer

Within the context of Kafka, 'Reliability' refers to the ability of a cluster to ensure that messages are delivered correctly and without loss, whereas 'Availability' refers to the ability of a cluster to be accessible and responsive to clients data requests  (even when nodes within the were to unexpectedly die and/or when there is a lot of network traffic). Let's now go over each of the listed features and decide whether or not they support reliability and/or availability:
- *Topic Replication* - **True**. Topic replication ensures that the messages within a single copy are copied across two or more brokers within the Kafka cluster, which means that should one o, another broker will be able to take its place. This storage redunancy within the cluster ensures that data is reliably stored.
- *Topic Partitioning* - **True**. By partitioning a single topic into multiple partitions, multiple consumers are able to consume messages from that topic simultaneously, which minimises the time consumers need to wait for other consumers to finish reading a topic before they start reading. 
- *Consumer Group ID* - **True**. Allowing for multiple consumers existing within a single group to read from the same topic provides fault tolerance: if one of the consumers in this group were to die while consuming a topic, another idle consumer within the group would be able to take its place and finish the reading process.
- *Ack All* - **True**. 'Ack All' ensures that when a producer sends new messages to a topic, the leader and all of the follower brokers have successfully stored these new messages. This ensures that these messages have been successfully replicated within the cluster and, therefore, ensures that the messages have been reliably stored.

### Question 3

#### Question

Which of the following Kafka features support scaling?
- Topic Replication
- Topic Partitioning
- Consumer Group ID
- Ack All

#### Answer

'Scalability' refers to the ability of a Kafka cluster to handle increasing amounts of data and traffic, without degredations in performance or availability.  Let's now go over each of the listed features and decide whether or not they help with scalability: 
- *Topic Replication* - **True**. As of Kafka 2.4, it's possible for consumers to read topics not only from the leader replica nodes, but also from the follow replica nodes (see [here](https://stackoverflow.com/questions/37803376/consuming-from-a-replica) and [here](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica)). This means that increasing the replication factor of a topic increases the number of nodes that a consumer can read a particular topic from, which allows for increased consumer parallelism, which is very useful as the amount of consumer traffic increases.
- *Topic Partitioning* - **True**. Partitioning allows for multiple consumers to read from the same topic simultaneously, which allows for a Kafka cluster to remain performant, even as the amount of consumer traffic increases. 
- *Consumer Group ID* - **True**. Multiple consumers within a single group can read from multiple partitions within a topic simultaneously; this significantly speeds up read times and, thererfore, ensures the the Cluster works quickly, even when consumers must read millions of messages from a topic.
- *Ack All* - **False**. When 'Ack All' is specified, producers must wait for confirmation from all of the replica followers before writing their next batch of data to the topic. Although waiting for this confirmation increases robustness, this comes at the cost of latency and, therefore, scalability, since producers aren't able to perform as many writing operations within a set period of time; this may become an issue if we need to be able to write lots of data to our cluster quickly. Indeed, when choosing what level of acknowledgement to specify for our Kafka cluster, the main thing to consider is the trade-off between performance and reliability: lower acknowledgement levels allow for produces to write to the cluster faster, but are less reliable, whereas higher achnowledgement levels ensure more robvust topic replication, but means that producers take longer to write. The performance-reliability trade-off of different acknowledgement settings is explained in more detail [here](https://www.confluent.io/blog/configure-kafka-to-minimize-latency/#End-to-end_latency_vs._producer_and_consumer_latencies).


### Question 4

#### Question

Which of the following columns in the Green Taxi dataset are good candidates for being chosen as a topic partitioning key? Consider the cardinality and the scaling implications of the field(s) you select:
- `payment_type`
- `VendorID`
- `passenger_count`
- `total_amount`
- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`

#### Answer

When it comes to choosing a column to partition our streaming data by, we want to choose a key that ensures that our messages are distributed as evenly as possible over all of the partitions. Towards this end, we ought to choose a column that contains a **large range of different values** (i.e. has a high cardinality). 

##### Theoretical Background

To understand why this is the case, let's recall how Kafka uses a key value to decide which partition a message should be stored in:
1. A *hash function* is used to compute the hash value of the message's key. Importantly, hash functions are *deterministic*, meaning that they return the exact same hash value if given the same key value input.
1. The remainder after dividing this hash value by the number of partitions in our cluster is computed (i.e. `hash(key) % num_partitions`, where `hash(key)` denotes the hash function acting on our key value). The result of this calculation must be an integer from `0` to `num_partitions - 1` (i.e. there are `num_partition` possible results), so this value can be used to assign the message to a particular partition.
1. Because hash functions return the same hash value for identical inputs, this process is guaranteed to allocate messages with the same key to the same partition. This process does not gaurantee, however, that messages with different keys will be in placed different partitions, since:
    - Different key values may map to the same hash value output (i.e. the hash function could be a many-to-one mapping, rather than a one-to-one mapping)
    - Even if the hash values of the two key values are different, the remainder after dividing the different hash values by `num_partitions` might still be equal.

Now that we've recalled how Kafka uses key values to allocate messages, let's more formally describe this allocation process. Let $m$ denote the total number of unique values contained in our chosen partition key column, $n$ is the total number of unique hash values that can be generated from all the values in this key column, $p$ is the number of partitions in our cluster, and $\tilde{p}$ is the maximum number of different outputs that can be generated by the Kafka allocation algorithm we've just described (i.e. $\tilde{p}$ is the total number of partitions that Kafka is capable of allocating messages to using this algorithm).

Since each input into a hash function must map to exactly one hash value output, the number of different hash values $n$ that can be generated from $m$ different inputs must satisfy:

$$
n \leq m
$$

i.e. we can't generate more unique hash values than unique inputs, although we may produce fewer hash values than inputs if different inputs map to the same hash value. 

If $n$ is less than the number of partitions $p$, then the computation `hash(key) % num_partitions` will only be able to produce a maximum of $n$ different outputs: the 'maximal case' occurs if dividing each one of the $n$ different hash values by $p$ returns a different remainder value. Conversely, if $n$ is larger than $p$, then the maximum number of different outputs that can be produced by calculating `hash(key) % num_partitions` is $p$, since this modulus calculation can only output values between `0` and `num_partitions - 1` (i.e. $p$ different outputs). The maximum number of outputs that can be produced by Kafka's partition allocation procedure $\tilde{p}$ must satisfy then:

$$
\tilde{p} \leq 
\begin{cases}
n \text{ if } n \lt p \\
p \text{ if } p \leq n \\
\end{cases} = \text{min}(n, p)
$$

By combining this result with the fact that $n \leq m$, we deduce that:

$$
\text{min}(n, p) \leq \text{min}(m, p)
$$

which means that:
$$
\tilde{p} \leq \text{min}(m, p)
$$

So what is this formula saying? It's saying that the maximum number of partitions that Kafka can assign topics to using a partition key is limited by *either*:
1. The number of partitions in our cluster $p$
2. The number of unique values contained in our key column $m$

This means that if the number of partitions in our Kafka cluster $p$ is greater than the number of unique values in our key column $m$, then it's guaranteed that some of partitions will not be allocated any data. To avoid this situation, we want to choose a key column that contains many more unique values than there are partitions in our cluster (i.e. $m \gg p$): although this doesn't guarantee that all our partitions will be used (e.g. if our different key values might happen to map to the same hash value, or if the remainder after dividing each hash value by the number of partitions is the same), it does ensure that it's *not impossible* for us to utilise all our partitions.

To better illustrate this point, let's consider an 'extreme' case where we have two partitions, but our partition key column only contains one unique value. Since our key column only contains one unique value here, computing `hash(key) % num_partitions` will return the same result for each message, which means all of our messages will be allocated to the same partition, leaving the other partition empty. Although selecting a key column with two unique values wouldn't guarantee that both partitions are used, it's now at least *possible* that both partitions are used (i.e. if `hash(key) % 2` returns `0` for one of the unique values in the key column, and `1` for the other unique value).

##### Application of Theory to Question

Now that we understand how the number of partitions Kafka can assign messages to is related to the cardinality of our chosen partition key, let's now decide which of the columns listed in the original question are 'good partition key candidates'.

Since the `total_amount`, `lpep_pickup_datetime`, and `lpep_dropoff_datetime` are all continuous quantities, they should contain a large number of unique values (i.e. have a high cardinality) and, therefore, be **good partition key candidates**. To confirm that this is the case, we can use the `print_csv_cardinality` convenience function defined in the `utils.py` file inside this directory (make sure you've run `pip install -r requirements.py` first):
```bash
CSV_URL='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz' && \
python3 -c "from utils import print_csv_cardinality; print_csv_cardinality('${CSV_URL}', column='total_amount')" && \
python3 -c "from utils import print_csv_cardinality; print_csv_cardinality('${CSV_URL}', column='lpep_pickup_datetime')" && \
python3 -c "from utils import print_csv_cardinality; print_csv_cardinality('${CSV_URL}', column='lpep_dropoff_datetime')"
```
This prints:
```
Number of unique values in 'total_amount' column: 7475
Number of unique values in 'lpep_pickup_datetime' column: 541269
Number of unique values in 'lpep_dropoff_datetime' column: 540701
```
Based on our previous theoretical analysis, any one of these keys *could possibly* support thousands of partitions, which is useful should we want to scale-up our cluster by adding more partitions.

On the other hand, the `payment_type`, `VendorID`, and `passenger_count` appear to contain a significantly smaller number of unique values; we can see this by executing:
```bash
CSV_URL='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz' && \
python3 -c "from utils import print_csv_cardinality; print_csv_cardinality('${CSV_URL}', column='payment_type')" && \
python3 -c "from utils import print_csv_cardinality; print_csv_cardinality('${CSV_URL}', column='VendorID')" && \
python3 -c "from utils import print_csv_cardinality; print_csv_cardinality('${CSV_URL}', column='passenger_count')"
```
This prints:
```bash
Number of unique values in 'payment_type' column: 5
Number of unique values in 'VendorID' column: 2
Number of unique values in 'passenger_count' column: 10
```
These columns are, therefore, **poor partition key candidates**, since they'd only be able to support a relatively small number of partitions. Indeed, if we chose the `VendorID` column as a paritition key, only two of our partitions would actually store data.

##### Limitations of Looking at Cardinality Alone

In answering Question 4, we only considered the cardinality of each column when deciding if that column is appropriate to be used as a message key. The cardinality of a column, however, is not the only thing we should consider when deciding whether that column's values would be appropriate message keys. We should, for instance, also consider the *distribution* of values within the column: intuitively, columns with more uniformly distributed will tend to produce a more uniform distribution of hash values and, therefore, should result in a more equal distribution of data among the partitions in our cluster.

### Question 5

#### Question

Which options *should* be specified when creating a Kafka Consumer, but *don't* need to be provided when creating a Kafka Producer?
- A Deserializer
- A Topic to Subscribe to
- A Bootstrap Server
- A Group ID
- An Offset
- A Cluster Key and/or Cluster Secret

#### Answer

Let's go through each of these options in turn:

- *A Deserializer* - **True**. Recall that before a producer writes data to a Kafka cluster, it first must serialize this data into bytes; when this data is subsequently read by a consumer, the key and values of the read message are still in bytes and, therefore, must be deserialized to a useable format (e.g. a string). In summary then, producers need to be supplied with a serializer, but consumers need to be given a deserializer.
- *A Topic to Subscribe to* - **True**. Although we *do* need to specify which topic a producer should write data to, a producer doesn't 'subscribe' to a topic: by definition, only consumers 'subscribe to' (i.e. read) from topics.
- *A Bootstrap Server* - **False**. Within the context of a Kafka cluster, bootstrap server(s) are the initial server(s) you connect to when to connect to a Kafka cluster. The purpose of each bootstrap server is to list the full set of broker nodes that exist within the Kafka cluster. Since connecting to a bootstrap server is required to establish a connection to the cluster itself, *both* consumers and producers need to be specified with a bootstrap server.
- *A Group ID* - **True**. Producers are *not* organised into groups, whereas Consumers *are*; consequently, we need to provide a Group ID when creating a consumer, but not when creating a producer.
- *An Offset* - **True**. When Producers write data to a Kafka cluster, they append that new data to the *end* of the topic (i.e. the new data is added on starting from the *latest* offset index), which means they *don't* need to be provided with an initial offset. Conversely, consumers can read data starting from the *very first* message in the topic, or from the *latest* message sent to that topic. To ensure that , one should specify the initial offset of a consumer;
- *A Cluster Key and/or Cluster Secret* - **False**. If a key or password is needed to access a cluster, *both* consumers and producers connecting to that cluster must be provided with this key/secret.

## Part 2: Practical Implementation Questions

### Question 

Implement a Kafka streaming application that reads ride records stored in the following CSVs:
1. `fhv_tripdata_2019-01.csv.gz` (available at [this link]())
1. `green_tripdata_2019-01.csv.gz` (available at this [this link]())
and uploads them into their own separate Kafka topics, one for each CSV (e.g. the trips read from `fhv_tripdata_2019-01.csv.gz` are placed into the `fhv_trips` topic, and the trips read from `green_tripdata_2019-01.csv.gz` are placed into the `green_trips` topic).

Your code should include the following components:
1. A Kafka Producer that reads each Taxi CSV file, and uploads the read data into a specified Kafka topic.
1. A Kafka Consumer that can read from the topics written to by your Kafka producer.
1. A PySpark Streaming application that reads from both the Green taxi and FHV taxi topics created in Kafka, merges them into a single dataframe, and then writes this merged dataframe into a new Kafka topic (i.e. an '`all_rides`' topic). Appropriate transformations and aggregations should then be applied to the merged dataframe to rank the popularity of each `PUlocationID` across both the Green and FHV trip datasets.

Note that it is not necessary to find an exact number for the popularity of each `PUlocationID` across both datasets, so if you encounter memory related issue, it's fine to analyse only a smaller subset of the records in each CSV.

### Answer

The `03-streaming-app.ipynb` notebook in this directory implements a streaming application that meets all the requirements specified in the question. The notebook itself utilises data and code that is defined within the following files and directories:

- The `schema/` directory houses numerous [Apache Arvo](https://en.wikipedia.org/wiki/Apache_Avro) files that specify the schema for the message keys and values we generate for our Kafka Cluster. It's worth noting that the message keys and values for the FHV taxi dataset and the Green taxi dataset are different, and hence require different schemas. This means that `schema/` contains four Arvo files: one for the FHV message key, another for the FHV message value, one for the Green message key, and another for the Green message value.
- The `producers/` directory contains two Python-based implementations of Kafka producers that can write Taxi ride data in CSV format to a Kafka cluster:
    1. `producers/confluent_producer.py` contains a Kafka producer implemented using the `confluent` Python package; this implementation uses the Arvo schema defined inside of the `schema/` subsirectory to help serialize each taxi record.
    1. `producers/kafka_python_producer.py` contains a Kafka producer implemented using the `kafka-python` package; this implementation serializes the each taxi record by appending all the values together as a string, and then serializing this single string.
- The `consumers/` directory contains two Python-based implementations of Kafka consumers that can read messages written to our Kafka cluster by the producers defined in `producers/`:
    1. `consumers/confluent_consumer.py` contains a Kafka consumer implemented using the `confluent` Python package; this consumer is capable of reading messages produced to a topic by the `producers/confluent_producer.py` producer.
    1. `consumers/kafka_python_consumer.py` contains a Kafka consumer implemented using the `kafka-python` Python package; this consumer is capable of reading messages produced to a topic by the `producers/kafka_python_producer.py` producer.
- `streaming.py` contains functions to read, write, and transform PySpark Streaming Dataframes.
- `utils.py` contains a variety of utility functions; some of these are called within the `streaming.ipynb` notebook.

For further details about any one of these components, please refer to the documentation within the respective code file.

Before opening the `03-streaming-app.ipynb` notebook, however, we'll need to start up a local Kafka cluster. To do this, run:
```bash
docker compose up -d
```
**inside of the `06-stream/homework` directory**; this will execute the Docker containers defined in `docker-compose.yml`; the configuration settings associated with this Kafka cluster are listed in `kafka_settings.yml`.
