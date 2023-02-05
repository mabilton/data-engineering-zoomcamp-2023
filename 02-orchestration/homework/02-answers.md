

## Setting Up GCS

Use Terraform; for convenience, we've copied over the `terraform` directory from `01-intro/homework` to `02-orchestration/homework`;

First run:
```bash
terraform init
```

followed by
```bash
terraform apply
```

should set up the GCS Bucket and BigQuery database we need. 

## Question 1

### Question

### Answer


Create GCP credential block:
```
cd blocks && \
python3 -m gcp_cred \
--credentials=/home/mabilton/clear-nebula-375807-143b9f4e2461.json \
--block_name='gcp-cred-zoomcamp' && \
cd ..
```

Create GCS bucket block:
```bash
cd blocks && \
python3 -m gcs_bucket \
--bucket='zoomcamp_data_lake_clear-nebula-375807' \
--cred_block='gcp-cred-zoomcamp' \
--block_name='gcs-bucket-zoomcamp' && \
cd ..
```

Can then run flow:
```
cd flows && \
python3 -m etl_to_gcs \
--color='green' \
--months=1 \
--year=2020 \
--save_dir='data' \
--gcs_block='gcs-bucket-zoomcamp' && \
cd ..
```

In the logs, we should find the line:
```bash
17:24:43.300 | INFO    | Task run 'clean-0' - rows: 447770
```

## Question 2

### Question

### Answer

Cron uses the following syntax to specify times for scheduled events:
```bash
MIN HR DAY MON WK 
```
where:
- `MON` is either an int, or `*`, which specify that the scheduled operation should be run every month.


With this in mind, the `cron` syntax we should use to run a scheduled job on the first day of every month at 5 AM is:
```
0 5 1 * *
```
Let's break this down:
- The first `0` specifies that the 
- The `5` specified
-
- The next two `*` wildcards specify that should occur for any week and any month.

To create a deployment that runs `etl_to_gcs.py` using this `cron`, we can use the CLI:
```bash
prefect deployment build \
-n etl_gcs_flow \
--cron "0 5 1 * *" \
--apply flows/etl_to_gcs.py:etl_web_to_gcs 
```
We note from the output of this:
```
Deployment 'etl-web-to-gcs/etl_gcs_flow' successfully created with id '04f61713-d2fd-44c9-a846-7066ffae3f7a'.
```

## Question 3

### Question

### Answer


Need to upload Yellow taxi data for Feb. 2019 and March 2019 to GCS using `etl_to_gcs.py`. As a simple way to do this without creating
any additional deployments, we can run from the commandline:
```bash
cd flows && \
python3 -m etl_to_gcs \
--color='yellow' \
--months=[2,3] \
--year=2019 \
--save_dir='data' \
--gcs_block='gcs-bucket-zoomcamp' && \
cd ..
```

Please note that the Yellow Taxi datasets are substatially larger than the Green taxi datasets, so this will take noticably longer than our previous execution of the `etl_web_to_gcs` flow. 

Once this is successfully completed, we can create a deployment:

```bash
prefect deployment build \
-n etl_bq_flow \
--apply flows/etl_to_bq.py:etl_gcs_to_bq  
```
From the output of this we note:
```bash
Deployment 'etl-gcs-to-bq/etl_bq_flow' successfully created with id '2d47c586-8d66-4dd8-82a6-e54e67d8c4b9'.
```
Can start Prefect agent in anticipation of running this flow:
```bash
prefect agent start -q 'default'
```

To execute this deployment, we can run:
```bash
prefect deployment run \
--param color="yellow" \
--param year=2019 \
--param months=[2,3] \
--param save_dir='data' \
--param gcs_block='gcs-bucket-zoomcamp' \
```

Upon inspecting the logs for this deployment (either through the CLI or the Orion UI), we should see the following output printed near the very end:
```bash
00:26:39.616 | INFO    | Flow run 'dramatic-pig' - Total number of rows processed = 14851920
```

## Question 4

### Question

### Answer

```bash
cd blocks && \
python3 -m github \
--repo=https://github.com/mabilton/data-engineering-zoomcamp-2023 \
--block_name='gh-zoomcamp' && \
cd ..
```

```
cd ../.. && \
prefect deployment build \
-n github-etl \
-sb github/gh-zoomcamp \
--apply 02-orchestration/homework/flows/etl_to_gcs.py:etl_web_to_gcs && \
cd 02-orchestration/homework
```
Name of deployment:
```bash
Deployment 'etl-web-to-gcs/github-etl' successfully created with id '498e7c03-3724-45d5-82fb-1ae98cba1e85'.
```

```bash
prefect deployment run \
--param color="yellow" \
--param year=2019 \
--param months=[2,3] \
--param save_dir='data' \
--param gcs_block='gcs-bucket-zoomcamp' \
etl-web-to-gcs/github-etl
```

To make sure this flow is actually running using the Github code, we can delete `` and re-run the above deployment; we should see that this still works.

Upon inspecting the logs, we see that:
```

```

## Cleaning Up

To prevent, we should make sure to shut down any using Terraform:
```bash
terraform destroy
```