# Week 4 Homework Questions

This file lists the homework questions for Week 4 for the 2023 cohort of the Data Engineering Zoomcamp; please refer to `02-answers.md` within this subdirectory for the answers to each question.

## Set-Up

Before answering any questions in this week's homework, you first need to upload the following datasets to a GCP Bucket:
1. All of the [Yellow taxi data](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow) for 2019 and 2020
1. All of the [Green taxi data](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green) for 2019 and 2020
1. All of the [FHV taxi data](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) for 2019.

Once this data is uploaded, you then need to create a `dbt` project that creates the following tables in a BigQuery database:
1. `stg_green_tripdata`, which stages the Green taxi data uploaded to the GCP Bucket.
1. `stg_yellow_tripdata`, which stages the Yellow taxi data uploaded to the GCP Bucket.
1. `stg_fhv_tripdata`, which stages the FHV taxi data uploaded to the GCP Bucket.
1. `fact_trips`, which stores all of the Yellow and Green taxi data and is formed by transforming the `stg_green_tripdata` and `stg_yellow_tripdata` tables.
1. `fact_fhv_trips`, which stores all of the FHV taxi data and is formed by transforming the `stg_fhv_tripdata` table.
1. `dim_zones`, which stores the [Taxi Zone Lookup table](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/misc); this table can be stored as a `seed` in the `dbt` project, as opposed to uploading it to a GCP Bucket.

Finally, you should create a Google Data Studio dashboard that can be used to visualise the data in the BigQuery tables created by your `dbt` project.

## Question 1

What is the count of records in the model `fact_trips` after running all models and filtering for trips which begin in 2019 and 2020 only?

## Question 2

What is the distribution between service type (i.e. Yellow taxi vs Green taxi) fpr the 2019 and 2020 data stored in `fact_trips`? Answer this question using an appropriate visualisation in your Google Data Studio dashboard. 

## Question 3

What is the count of records in the model `stg_fhv_tripdata` after running all models?

## Question 4

What is the count of records in the model `fact_fhv_trips` after running all models?

## Question 5

During which month did the greatest number of FHV taxi trips occur? Answer this question by creating a Google Data Studio visualisation using the `fact_fhv_trips` table.
