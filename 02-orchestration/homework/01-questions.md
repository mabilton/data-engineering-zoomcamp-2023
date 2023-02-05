# Week 2 Homework Questions

This file lists the homework questions for Week 1 for the 2023 cohort of the Data Engineering Zoomcamp; please refer to `02-answers.md` within this subdirectory for the answers to each question.

This week's homework focuses on using Prefect to orchestrate data engineering workflows.

1. `etl_web_to_gcs.py`
2. `

## Question 1

Using `etl_web_to_gcs.py` as a template, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. How many rows does that dataset have? Look at the flow logs to answer this question.

## Question 2

`cron` is a common scheduling specification for workflows. Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What `cron` schedule should be used here?

## Question 3

Using `etl_gcs_to_bq.py` as a template, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. 

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

## Question 4

Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

## Question 6

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?