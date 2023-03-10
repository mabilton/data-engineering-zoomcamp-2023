# Piperider Workshop Homework Questions

This file lists the Piperider workshop homework questions for the 2023 cohort of the Data Engineering Zoomcamp; please refer to `02-answers.md` within this subdirectory for the answers to each question.

As an overview, this homework is basically an extension of the Week 4 homework on `dbt`. More specifically, it focuses on using `piperider` within a `dbt` project in order to generate summary reports about our data. 

Additionally, instead of hosting our data on BigQuery, we'll instead be using [`duckdb`](https://duckdb.org/) as our local database management system (DBMS). Unlike Postgres, which we previously used in the Week 1 homework, `duckdb` does not require us to start up a server process to connect to in a separate terminal instance. Instead, `duckdb` is an **in-process DBMS**: whenever we query our `duckdb` database (e.g. by using the `python` API), the `duckdb` process which runs our query is started inside of the process that is running our `python` code, which means we don't need to start a separate terminal terminal process to run our query. For 'small-scale' databases, this in-process behaviour is obviously very convenient.

## Set-Up

To run this homework, you'll need to install `dbt`, `duckdb`, and `piperider`; you can easily install all of these requirements with the help of the `requirements.txt` file in this homework directory:
```bash
pip install -r requirements.txt
```

Next, you'll need to download the same set of CSV files we used for the Week 4 homework; these CSV files consisted of:
1. The Yellow Taxi trip data for 2019 **and** 2020, which can be found here.
1. The Green Taxi trip data for 2019 **and** 2020, which can be found here.
1. The FHV Taxi trip data for 2019, which can be found here.
Once downloaded, these files should be ingested into your `duckdb` database. For more information on how to ingest data into a `duckdb` database, please refer to the `duckdb` documentation.

After this, you should copy over the Week 4 homework `dbt` project, since we'll be 'building off' of this code. Before running this project, you'll need to make two modifications:
1. Modify the `profiles.yml` file so that the `dbt` project is correctly configured to work with a `duckdb` database rather than a BigQuery database.
1. Change the `fact_trips` and `fact_fhv_trips` models so that they only include trips that ocurred in 2019 and 2020 (i.e. filter out trips with outlier date values).

Finally, you should run this `dbt` project and then use `piperider` to generate a report. By looking over the summary statistics in this report, you should be able to answer all of the homework questions.

## Question 1

What is the distribution of values for `vendor_id` in the `fact_trips` table? Choose from the following options:
- 70.1%, 29.6%, 0.5%
- 60.1%, 39.5%, 0.4%
- 90.2%, 9.5%, 0.3%
- 80.1%, 19.7%, 0.2%

## Question 2

How many records in `fact_trips` have a positive, zero, and negative `total_amount` value? Choose from the following options:
- 51.4M positive, 15K zero, 48.6K negative
- 21.4M positive, 5K zero, 248.6K negative
- 61.4M positive, 25K zero, 148.6K negative
- 81.4M positive, 35K zero, 14.6K negative

## Question 3

What are the numerical summary statistics of the `trip_distance` values in the `fact_trips` table? Choose from the following options:
- Average: 1.95, Standard Deviation: 35.43, Min: 0, Max: 16.3K, Sum: 151.5M
- Average: 3.95, Standard Deviation: 25.43, Min: 23.88, Max: 267.3K, Sum: 281.5M
- Average: 5.95, Standard Deviation: 75.43, Min: -63.88, Max: 67.3K, Sum: 81.5M
- Average: 2.95, Standard Deviation: 35.43, Min: -23.88, Max: 167.3K, Sum: 181.5M
