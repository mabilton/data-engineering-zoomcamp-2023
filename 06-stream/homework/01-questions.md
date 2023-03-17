# Week 6 Homework Questions

This file lists the homework questions for Week 6 for the 2023 cohort of the Data Engineering Zoomcamp; please refer to `02-answers.md` within this subdirectory for the answers to each question.

This week's homework consists of two distinct sections:
1. Section 1 tests your theoretical knowledge of Kafka and streaming concepts.
1. Section 2 tasks you with implementing a Streaming application in Kafka.

## Section 1: Theoretical Questions

The first part of this homework consist of theoretical questions on Kafka and streaming concepts.

### Question 1

Please select all of the statements which are correct:
- Kafka Nodes are responsible for storing topics
- Zookeeper is to be removed from Kafka 4.0 and onwards
- Message retention ensures that messages do not get lost over specific period of time.
- Group IDs allow for messages to be consumed by multiple distributed consumers simultaneously


### Question 2

Which of the following Kafka features support reliability and availability?
- Topic Replication
- Topic Paritioning
- Consumer Group ID
- Ack All


### Question 3

Which of the following Kafka features support scaling?
- Topic Replication
- Topic Paritioning
- Consumer Group ID
- Ack All

### Question 4

Which of the following columns in the Green Taxi dataset are good candidates for being chosen as a topic partitioning key? Consider cardinality and scaling implications of the field(s) you select:
- `payment_type`
- `vendor_id`
- `passenger_count`
- `total_amount`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`


### Question 5

Which options must be specified when creating a Kafka Consumers, but don't need to be provided when creating a Kafka Producer?
- A Deserializer
- A Topic to *Subscribe* to
- A Bootstrap Server
- A Group ID
- An Offset
- A Cluster Key and/or Cluster-Secret


## Section 2: Practical Implementation

In the second part of this homework, we'll actually be implementing a Kafka streaming application.

Implement a Kafka streaming application that reads ride records from the following CSVs:
1. `fhv_tripdata_2019-01.csv.gz` (available at this link)
2. `green_tripdata_2019-01.csv.gz` (available at this link)
and these records into their own Kafka topic (e.g. the trips read from `fhv_tripdata_2019-01.csv.gz` are placed into the `fhv_trips` topic, and the trips read from `green_tripdata_2019-01.csv.gz` are placed into the `green_trips` topic).

Your code should include the following components:
1. A Kafka Producer that reads each Taxi CSV file, and uploads .
2. A PySpark Streaming application that reads from both the Green taxi and FHV taxi topics created in Kafka, merges them into a single dataframe, and then writes this merged dataframe into a new Kafka topic (i.e. an '`all_rides`' topic). Appropriate transformations and aggregations should then be applied to the merged dataframe to rank the popularity of each `PUlocationID` across both the Green and FHV trip datasets.

Note that it is not necessary to find an exact number for the popularity of each `PUlocationID` across both datasets, so if you encounter memory related issue, it's fine to analyse only a smaller subset of the records in each CSV.