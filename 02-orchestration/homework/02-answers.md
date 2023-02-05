

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

```bash
17:24:41.393 | INFO    | prefect.engine - Created flow run 'smoky-pig' for flow 'etl-web-to-gcs'
17:24:41.562 | INFO    | Flow run 'smoky-pig' - Created task run 'fetch-0' for task 'fetch'
17:24:41.563 | INFO    | Flow run 'smoky-pig' - Executing 'fetch-0' immediately...
/home/mabilton/Documents/Notes/data-engineering-zoomcamp-2023/02-orchestration/homework/etl_to_gcs.py:10: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
17:24:43.054 | INFO    | Task run 'fetch-0' - Finished in state Completed()
17:24:43.089 | INFO    | Flow run 'smoky-pig' - Created task run 'clean-0' for task 'clean'
17:24:43.090 | INFO    | Flow run 'smoky-pig' - Executing 'clean-0' immediately...
17:24:43.298 | INFO    | Task run 'clean-0' -    VendorID lpep_pickup_datetime lpep_dropoff_datetime store_and_fwd_flag  ...  total_amount  payment_type  trip_type  congestion_surcharge
0       2.0  2019-12-18 15:52:30   2019-12-18 15:54:39                  N  ...          4.81           1.0        1.0                   0.0
1       2.0  2020-01-01 00:45:58   2020-01-01 00:56:39                  N  ...         24.36           1.0        2.0                   0.0

[2 rows x 20 columns]
17:24:43.299 | INFO    | Task run 'clean-0' - columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
17:24:43.300 | INFO    | Task run 'clean-0' - rows: 447770
17:24:43.323 | INFO    | Task run 'clean-0' - Finished in state Completed()
17:24:43.347 | INFO    | Flow run 'smoky-pig' - Created task run 'write_local-0' for task 'write_local'
17:24:43.348 | INFO    | Flow run 'smoky-pig' - Executing 'write_local-0' immediately...
17:24:44.353 | INFO    | Task run 'write_local-0' - Finished in state Completed()
17:24:44.390 | INFO    | Flow run 'smoky-pig' - Created task run 'write_gcs-0' for task 'write_gcs'
17:24:44.391 | INFO    | Flow run 'smoky-pig' - Executing 'write_gcs-0' immediately...
17:24:44.555 | INFO    | Task run 'write_gcs-0' - Getting bucket 'zoomcamp_data_lake_clear-nebula-375807'.
17:24:45.313 | INFO    | Task run 'write_gcs-0' - Uploading from PosixPath('data/green/green_tripdata_2020-01.parquet') to the bucket 'zoomcamp_data_lake_clear-nebula-375807' path 'data/green/green_tripdata_2020-01.parquet'.
17:24:46.245 | INFO    | Task run 'write_gcs-0' - Finished in state Completed()
17:24:46.286 | INFO    | Flow run 'smoky-pig' - Finished in state Completed('All states completed.')
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
prefect deployment build etl_to_gcs.py:etl_web_to_gcs -n etl_gcs_flow --cron "0 5 1 * *" -a
```

## Question 3

### Question

### Answer

Need to upload Yellow taxi data for Feb. 2019 and March 2019 to GCS using `etl_to_gcs.py`. As a simple way to do this without creating
any additional deployments, we can run from the commandline:
```bash
python3 -c 'from etl_to_gcs import etl_web_to_gcs; etl_web_to_gcs(color="yellow", year=2019, months=[2, 3])'
```

Please note that the Yellow Taxi datasets are substatially larger than the Green taxi datasets, so this will take noticably longer than our previous execution of the `etl_web_to_gcs` flow. 

Once this is successfully completed, we can create a deployment:

```bash
prefect deployment build etl_to_bq.py:etl_gcs_to_bq -n etl_bq_flow -a
```

```bash
prefect agent start -q 'default'
```

To execute this deployment, we can run:
```
prefect deployment run --param color="yellow" --param year=2019 --param months=[2,3] etl-gcs-to-bq/etl_bq_flow
```

Upon inspecting the logs for this deployment (either through the CLI or the Orion UI), we should see the following output printed near the very end:
```bash
Total number of rows processed = 14851920
```

## Question 4

### Question

### Answer



## Cleaning Up

To prevent, we should make sure to shut down any using Terraform:
```bash
terraform destroy
```