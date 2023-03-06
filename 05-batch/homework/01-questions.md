# Week 5 Homework Questions

This file lists the homework questions for Week 5 for the 2023 cohort of the Data Engineering Zoomcamp; please refer to `02-answers.md` within this subdirectory for the answers to each question.

## Set-Up

This week's homework focuses on using `pyspark`, so you'll need to install `spark` in order for everything to work. For instructions on how to install `spark`, please refer to the [instructions in the Data Engineering Zoomcamp repository](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_5_batch_processing/setup).

You'll also need to download the [FHVHV taxi data set for June 2021](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz); this CSV file is the primary dataset we'll be using for this homework. To answer Question 6, however, you'll also need to download the [Taxi Zones lookup table](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv).

## Question 1

Create a `spark` session with `pyspark` and call `spark.version`. What's the output? Choose from the following options:
- 3.3.2
- 2.1.4
- 1.2.3
- 5.4 

## Question 2

Create a `pyspark` dataframe for the downloaded FHVHV CSV into `spark`, and repartition it into 12 partitions. After this, save the data to Parquet. What is the average size of the 12 created Parquet files (i.e. the files ending with a `.parquet` extension)?

## Question 3

How many taxi trips started on June 15 in the FHVHV dataset you downloaded?

## Question 4

How much time (measured in hours) did the longest taxi trip in the dataset take?

## Question 5

Which port on your local machine do you use to access the Spark User Interface dashboard?

## Question 6

With the help of the Taxi Zone Lookup table, find the name of the most frequent pickup zone location.