# Week 1 Homework Questions

This file lists the homework questions for Week 1 for the 2023 cohort of the Data Engineering Zoomcamp; please refer to `02-answers.md` within this subdirectory for the answers to each question.

## Part A: Docker and Postgres

The first part of this homework focuses on using Docker and Postgres. The original homework questions for Part A can be found [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_docker_sql/homework.md) within the Data Engineering Zoomcamp repository.

### Question 1

To get help on a particular `docker` command, one can use the the `docker --help` command like so:

```
docker --help command
```
where `command` is the name of the command we want information on. Use `docker --help` to get help on the `docker build` command. Within the documentation that is printed to the console, which option has the following description:
```
Write the image ID to the file
```
Choose from one of the following four options:
1. `--imageid string`
1. `--iidfile string`
1. `--idimage string`
1. `--idfile string`

### Question 2

Run docker with the `python:3.9` image in an interactive mode and the entrypoint of `bash`. Now check the `python` modules that are installed (use `pip list`). How many `python` packages/modules are installed?

### Intermission: Prepare Postgres Database

In order to answer the following questions, you will need to create a Postgres databa,se and ingest the CSV data found at:
```
https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
```
which contains all of the 'Green' taxi ride data for New York, for the as well as the CSV data found at:
```
https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```
which contains the numerical ID for each geographical zone in New York.

### Question 3

What was the total number of taxi trips that started **and** finished on 2019-01-15? Remember that the `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the timestamp format (i.e. `date` and `hour+min+sec`).

### Question 4

On which day did the longest taxi trip in terms of *distance* (not time) occur? Use the pick up time for your calculations (as opposed to the drop-off time).

### Question 5

On 2019-01-01, how many trips had 2 passengers, and how many trips had 3 passengers?

### Question 6

Among all of the passengers picked up in the Astoria Zone, what was the drop-off zone of the trip that gave the largest tip (i.e. 'tip' as in 'monetary tip', *not* 'trip')? Note that we want the name of the zone, not the numerical id.

## Part B: Terraform and GCP

In the second part of the homework, we're going to check that we can correctly create resources on GCP using Terraform. The original homework questions for Part B can be found [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_terraform/homework.md) within the Data Engineering Zoomcamp repository.

### Question 1

Copy the `main.tf` and `variables.tf` Terraform files from [this directory](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform) within the Data Engineering Zoomcamp Github repository. For convenience, these files have already been copied into this repository, and can be found within the `01-intro/homework/terraform` directory. Note that you may wish to change the value of the `"region"` variable within `variables.tf` to a GCP region which is closer to your current geographical location. 

After creating a project on GCP and granting the correct permissions, run `terraform apply` to create the GCP resources defined in the `main.tf` file (i.e. a Google Cloud Storage Bucket and a BigQuery Table). 

Copy down the output that is printed to the terminal after running `terraform apply`.
