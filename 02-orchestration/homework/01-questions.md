# Week 2 Homework Questions

This file lists the homework questions for Week 2 for the 2023 cohort of the Data Engineering Zoomcamp; please refer to `02-answers.md` within this subdirectory for the answers to each question.

There are two points to note before reading these questions:
1. The Taxi data referenced in the questions refers to the CSV files hosted by `DataTalksClub` at the [`DataTalksClub/nyc-tlc-data` GitHub repository](https://github.com/DataTalksClub/nyc-tlc-data).
1. Some of the questions make reference to using the `etl_web_to_gcs.py` and `etl_gcs_to_bq.py` Python scripts as initial templates; these scripts can be found inside the [`flows/02-gcp` directory of the `discdiver/prefect-zoomcamp` Github repository](https://github.com/discdiver/prefect-zoomcamp/tree/main/flows/02_gcp).

## Question 1

Using [`etl_web_to_gcs.py` as a template](https://github.com/discdiver/prefect-zoomcamp/blob/main/flows/02_gcp/etl_web_to_gcs.py), create a Prefect flow that pulls Taxi data from the `DataTalksClub/nyc-tlc-data` repository, and then uploads this data into a Google Cloud Storage (GCS) Bucket as a [Parquet file](https://en.wikipedia.org/wiki/Apache_Parquet). The flow should accept a taxi colour, a year, and a list of months as inputs (i.e. the flow should be able to pull multiple months worth of data for a specific year and taxi colour if requested). Include a `print` statement in the flow that indicates how many rows the flow has processed.

Run this flow to pull Green taxi data for January 2020; how many rows does this dataset have?

## Question 2

[`cron`](https://en.wikipedia.org/wiki/Cron) is a very commonly used *job scheduling* tool (i.e. a piece of software that automatically runs a specified program at a particular time). 

Using the flow you previously created in Question 1, create a Prefect deployment that uses `cron` to automatically execute on the first of every month at 5am UTC. What `cron` schedule should be specified to achieve this behaviour?

## Question 3

Using [`etl_gcs_to_bq.py` as a template](hhttps://github.com/discdiver/prefect-zoomcamp/blob/main/flows/02_gcp/etl_gcs_to_bq.py), write a Prefect flow that pulls Taxi data Parquet files from a GCS bucket (like the Parquet files we uploaded to our GCS Bucket using the Prefect flow in Question 1), and uploads this data to a BigQuery database. Importantly, this Prefect flow should:
1. **Not** fill or remove rows with missing values. Additionally, the main flow should print the total number of rows processed by the script. 
1. Accept a list of months, a year, and a taxi colour as input parameters, just like the flow your wrote for Question 1.
1. Append data entries to the end of a BigQuery table if that table already exists in the database.

Once this flow is written, create a Prefect deployment that runs on your local machine and that uses the flow code that is locally stored on that same machine.

Run this deployment to pull the yellow taxi parquet data files for February 2019 **and** March 2019 from the GCS Bucket and subsequently upload these files to the BigQuery database. How many rows does your flow code process?

## Question 4

Create a GitHub storage block and use it to create a Prefect deployment that utilises the `etl_web_to_gcs.py` flow code *hosted on Github*, rather than using the `etl_web_to_gcs.py` flow code that's available on your local machine. 

Run the deployment on your local machine to upload the Green taxi data for November 2020 to your GCS Bucket; how many rows are processed by the flow code?

*Hint*: You will need to push the `etl_web_to_gcs.py` flow code to Github *before* running the deployment.

## Question 5

Connect to [Prefect Cloud](app.prefect.cloud) and create an Email notification block that automatically sends you an alert email when any flow is successfully completed. While connected to Prefect cloud, run the deployment created in Question 4 to pull the Green taxi data for April 2019 from Github and upload it into your GCS Bucket. How many rows are processed by the flow?

*Hint*: Once you connect your terminal to the Prefect Cloud environment, you'll need to recreate all of the blocks and deployments you created locally during Questions 1 through to 4. This is because the Prefect cloud environment our terminal has connected to is entirely independent from the Prefect environment we've locally been running on our machine (i.e. these Prefect environments don't share any of their blocks, deployments, variables, etc. by default).


## Question 6

In Prefect, Secret blocks are used to securely store values that should be kept secret (e.g. access keys). Create a secret block in the UI (either locally or in the cloud) that stores a fake 10-digit password. Once you’ve created your block, check how this 'password' is displayed when the block is clicked on. In particular, how many characters of this 10 character password are shown as asterisks (*)?