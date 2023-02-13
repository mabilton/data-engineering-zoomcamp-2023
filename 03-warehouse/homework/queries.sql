--
-- SET-UP
--

-- To replicate these queries in your own GCP project, note that you must:
--     1. Replace `clear-nebula-375807` with the name of your GCP project.
--     2. Replace `taxi_data` with the name of your BigQuery database
--     3. Replace `zoomcamp_data_lake/data/fhv_tripdata_*.csv.gz` with the 
--        path of the FHV data stored in your GCS bucket.

-- Create External Table:
CREATE OR REPLACE EXTERNAL TABLE `clear-nebula-375807.taxi_data.external_fhv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://zoomcamp_data_lake/data/fhv_tripdata_*.csv.gz']
);

-- Create Internal Table from External Table:
CREATE OR REPLACE TABLE `clear-nebula-375807.taxi_data.internal_fhv`
AS SELECT * FROM `clear-nebula-375807.taxi_data.external_fhv`;

--
-- QUESTION 1
--

-- Count number of rows in dataset:
SELECT COUNT(*) FROM clear-nebula-375807.taxi_data.internal_fhv;

--
-- QUESTION 2
--

-- Count distinct number of `dispatching_base_num` values, using internal table:
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `clear-nebula-375807.taxi_data.internal_fhv`;

-- Count distinct number of `dispatching_base_num` values, using external table:
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `clear-nebula-375807.taxi_data.external_fhv`;


--
-- QUESTION 3
--

-- Count number of rows with both blank `PUlocationID` and `DOlocationID`:
SELECT COUNT(*) FROM `clear-nebula-375807.taxi_data.internal_fhv`
WHERE PUlocationID IS NULL 
AND DOlocationID IS NULL;

--
-- QUESTION 5
--

-- Create Partitioned and Clustered Internal Table from Internal table:
CREATE OR REPLACE TABLE `clear-nebula-375807.taxi_data.optimized_fhv`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `clear-nebula-375807.taxi_data.internal_fhv`
);

-- Get distinct `affiliated_base_number` values for `dropoff_datetimes` between
-- '2019-03-01' and '2019-03-31', using the optimized table:
SELECT DISTINCT(affiliated_base_number) FROM `clear-nebula-375807.taxi_data.optimized_fhv`
WHERE DATE(dropoff_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- Get distinct `affiliated_base_number` values for `dropoff_datetimes` between
-- '2019-03-01' and '2019-03-31', using the non-optimized table:
SELECT DISTINCT(affiliated_base_number) FROM `clear-nebula-375807.taxi_data.internal_fhv`
WHERE DATE(dropoff_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

--
-- QUESTION 8
--

-- To replicate these queries in your own GCP project, note that you must:
--     1. Replace `clear-nebula-375807` with the name of your GCP project.
--     2. Replace `taxi_data` with the name of your BigQuery database
--     3. Replace `zoomcamp_data_lake/data/fhv_tripdata_*.parquet` with the 
--        path of the FHV data stored in your GCS bucket.

-- Create External Table from Parquet data:
CREATE OR REPLACE EXTERNAL TABLE `clear-nebula-375807.taxi_data.external_fhv_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp_data_lake/data/fhv_tripdata_*.parquet']
);

-- Create Internal Table using External Parquet Table: 
CREATE OR REPLACE TABLE `clear-nebula-375807.taxi_data.internal_fhv_parquet`
AS SELECT * FROM `clear-nebula-375807.taxi_data.external_fhv_parquet`;
