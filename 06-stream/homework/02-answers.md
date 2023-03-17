# Week 6 Homework Answers


# Part 1: Theoretical Questions

https://www.youtube.com/watch?v=SXQtWyRpMKs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=63

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
- `vendor_id`
- `passenger_count`
- `total_amount`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

#### Answer

When choosing which column should be used as a partition key, there are a few points to keep in mind:
1. *Number of partitions* - If we partition a topic by a column that contains millions of different values, then Kafka will have to produce millions of different partitions, which means that a significant number of these partitions will tend to be unused, which is wasteful. Additionally, Kafka needs to create new partitions for each new value encountered in the partitioning key; this means that we not only need to consider the number of values a column currently contains, but also the number of values we expect it to contain *in the future*. 
1. *Data distribution* - Ideally, the messages within a topic should be equally distributed amongst the partitions of that topic: this ensures that all of the partitions can be utilised when  consumer(s) read from that topic. Additionally, we'd prefer that any new data written to a topic is as evenly distributed amongst the topic partitions as possible. If this weren't the case, consumers would be only be able to read from a limited number of paritions to consume the latest messages within a topic, which reduces performance. 

With these two points in mind, let's evaluate how appropriate each of these columns are as partition keys: 

- `vendor_id` is a **good candidate** for being a partition key. This is because all of these columns contain a limited number of integer values and, therefore, partitioning by these columns means that an excessive number of partitions won't be formed. Additionally, the distribution of data within this column isn't excessively skewed towards particular values (at least within the `green_tripdata_2019-01.csv.gz` dataset, which we use in Question 6 of this homework). To confirm that the distribution of `vendor_id` values is, indeed, sufficiently uniform, we can use the `print_csv_values_count` convenience function that we've defined inside the `utils.py` file within this `homework` directory (**NB**: make sure you've run `pip install -r requirements.txt` before running the following command):
```bash
CSV_URL='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz' && \
python3 -c "from utils import print_csv_values_count; print_csv_values_count('${CSV_URL}', column='VendorID');"
```
This prints the following output to the terminal:
```
2    537235
1     93683
Name: VendorID, dtype: int64
```
i.e. approximately 85% of the data has a `VendorID` of `2`, whilst the rest of the data as a `VendorID` of `1`. Although this data distribution is obviously not uniform, it's probably 'good enough' for many practical purposes.

- The `payment_type` and `passenger_count` columns are, on the other hand, **bad candidates**:
```bash
CSV_URL='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz' && \
python3 -c "from utils import print_csv_values_count; print_csv_values_count('${CSV_URL}', column='payment_type');"
```
which prints:
```bash
1    388313
2    237857
3      3392
4      1330
5        26
Name: payment_type, dtype: int64
```

Similarly:
```bash
CSV_URL='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz' && \
python3 -c "from utils import print_csv_values_count; print_csv_values_count('${CSV_URL}', column='passenger_count');"
```
This prints to the terminal:
```bash
1    543508
2     44105
5     20550
6     10040
3      8175
4      3044
0      1462
8        16
9        11
7         7
Name: passenger_count, dtype: int64
```


`total_amount`, `tpep_pickup_datetime`, `tpep_dropoff_datetime` are all **bad choices** for partition keys; this is for a few of reasons:
1. All of these columns will contain, which means that a large number of partitions will be generated. 
1. It's almost certain that the number of partitions would expand over time. In the case of the `total_amount` column, inflation means that the cost of each taxi trip will tend to increase over time and, therefore, that new paritions would need to be generated 
1. 

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

Let's go thr
- *A Deserializer* - **True**. Consumers **must be provided** with a deserializer, whereas producers do not; instead, producers must be given a *serializer*. Recall that before a producer writes data to a Kafka cluster, it first must serialize this data into bytes; when this data is subsequently read by a consumer, the key and values of the read message are still in bytes, and the must be desrialized to a useable format (e.g. a string).
- *A Topic to Subscribe to* - **True**. Although we *do* need to specify which topic a producer should write data to, a producer doesn't 'subscribe' to a topic: by definition, only consumers 'subscribe to' (i.e. read) from topics.
- A Bootstrap Server - **False**. A bootstrap server must be specified for both Consumers and Producers
- A Group ID - **True**. Producers are *not* grouped into groups, but Consumers *are*; consequently, we need to provide a Group ID when creating a consumer, but not when creating a producer.
- *An Offset* - **True**. When Producers write data to a Kafka cluster, they append that new data to the *end* of the topic (i.e. the new data is added on starting from the *latest* offset index), which means they *don't* need to be provided with an initial offset. Conversely, consumers can read data starting from the *very first* message in the topic, or from the *latest* message sent to that topic. To ensure that , one should specify the initial offset of a consumer;

only new write data to the very end of the topic (i.e. larger offset index
). Conversely, we need to tell a consumer whether it should 
- *A Cluster Key and/or Cluster Secret* - **False**. If your Kafka Cluster is configured  

One **must specify** which topic a 
- *A Bootstrap Server* - **False**
- *A Group ID* - **True**
- *An Offset* - **True**
- *A Cluster Key and/or Cluster Secret* - **False**

## Part 2: Practical Implementation Questions

### Question

Implement a Kafka streaming application that reads ride records from the following CSVs:
1. `fhv_tripdata_2019-01.csv.gz` (available at this link)
2. `green_tripdata_2019-01.csv.gz` (available at this link)
and these records into their own Kafka topic (e.g. the trips read from `fhv_tripdata_2019-01.csv.gz` are placed into the `fhv_trips` topic, and the trips read from `green_tripdata_2019-01.csv.gz` are placed into the `green_trips` topic).

Your code should include the following components:
1. A Kafka Producer that reads each Taxi CSV file, and uploads .
2. A PySpark Streaming application that reads from both the Green taxi and FHV taxi topics created in Kafka, merges them into a single dataframe, and then writes this merged dataframe into a new Kafka topic (i.e. an '`all_rides`' topic). Appropriate transformations and aggregations should then be applied to the merged dataframe to rank the popularity of each `PUlocationID` across both the Green and FHV trip datasets.

Note that it is not necessary to find an exact number for the popularity of each `PUlocationID` across both datasets, so if you encounter memory related issue, it's fine to analyse only a smaller subset of the records in each CSV.

### Answer

```bash
docker compose up -d
```

Next, start a Jupyter



Once you're done, you'll want to ma
```bash
docker compose down
```
