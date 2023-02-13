# Week 3 Homework Questions

This file lists the homework questions for Week 3 for the 2023 cohort of the Data Engineering Zoomcamp; please refer to `03-answers.md` within this subdirectory for the answers to each question.

## Set-Up

Before answering any questions, create a data pipeline that pulls all of the F[HV Taxi data for 2019 from Github](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv), and uploads these compressed CSVs to a GCS bucket. To save space, these CSV files should not be uncompressed.

Once in a Bucket, write *queries in BigQuery* to **create both**:
1. An *external table* using the FHV data in the GCS bucket.
2. An *internal* (AKA 'standard') BigQuery table by using the previously created external table.

## Question 1

How many observations are contained within the FHV dataset for 2019?
- 65,623,481
- 43,244,696
- 22,978,333
- 13,942,414

## Question 2

Write a query to count the distinct number of `affiliated_base_number` values within the FHV table. 

What is the estimated amount of data that will be read when this query is executed on the External Table and on the Internal/'Standard' Table?
- 25.2 MB for the External Table and 100.87MB for the Internal Table
- 225.82 MB for the External Table and 47.60MB for the Internal Table
- 0 MB for the External Table and 0MB for the Internal Table
- 0 MB for the External Table and 317.94MB for the Internal Table

## Question 3

How many records have both a blank `PUlocationID` and a blank `DOlocationID` across the entire dataset?
- 717,748
- 1,215,687
- 5
- 20,332

## Question 4

If our queries always filter by `pickup_datetime` and order on `affiliated_base_number`, what would be the best way to cluster and/or partition the table to improve performance?
- Cluster on `pickup_datetime`, Cluster on `affiliated_base_number`
- Partition by `pickup_datetime`, Cluster on `affiliated_base_number`
- Partition by `pickup_datetime`, Partition by `affiliated_base_number`
- Partition by `affiliated_base_number`, Cluster on `pickup_datetime`

## Question 5

Create the clustered and/or partitioned table you suggested for Question 4, and then write a query that retrieves the distinct `affiliated_base_number` values for taxi rides whose  `pickup_datetime` is between `2019-03-01` and `2019-03-31` (inclusive).

How many bytes does BigQuery estimate will be used when this query is executed on the external table? Similarly, how many bytes does BigQuery estimate will be used when this query is executed on the clustered and/or partitioned table we just created? Pick the answer with the closest match: 
- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

## Question 6

Where is the data stored in the External Table you created?
- Big Query
- GCP Bucket
- Container Registry
- Big Table

## Question 7

"*It is best practice in BigQuery to always cluster your data*" - True or False?

## Question 8

It may be more efficient to store the FHV data as Parquet files instead of as CSVs. With this in mind, create a data pipeline (or extend the one you previously used) that pulls the FHV data from Github, converts it to Parquet, and then uploads it to a GCS Bucket.

Once in the GCS Bucket, create both an external table and an internal table in BigQuery using the Parquet data.  