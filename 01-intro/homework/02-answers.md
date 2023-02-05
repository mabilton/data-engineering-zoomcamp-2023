# Week 1 Homework Answers

This file contains my answers to the Week 1 homework questions for the 2023 cohort of the Data Engineering Zoomcamp. For convenience, each question is restated before giving the corresponding answer; the list of questions *without* corresponding answers can be found in `01-questions.md`, which can be found in the current directory.

## Part A

Part A of the homework can be found [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_docker_sql/homework.md) within the Data Engineering Zoomcamp repository.

### Question 1

#### Question

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

#### Answer

Calling `docker --help build` from the terminal prints the output:
```
Usage:  docker buildx build [OPTIONS] PATH | URL | -

Start a build

Aliases:
  docker buildx build, docker buildx b

Options:
      --add-host strings              Add a custom host-to-IP mapping (format: "host:ip")
      --allow strings                 Allow extra privileged entitlement (e.g., "network.host", "security.insecure")
      --attest stringArray            Attestation parameters (format: "type=sbom,generator=image")
      --build-arg stringArray         Set build-time variables
      --build-context stringArray     Additional build contexts (e.g., name=path)
      --builder string                Override the configured builder instance (default "default")
      --cache-from stringArray        External cache sources (e.g., "user/app:cache", "type=local,src=path/to/dir")
      --cache-to stringArray          Cache export destinations (e.g., "user/app:cache", "type=local,dest=path/to/dir")
      --cgroup-parent string          Optional parent cgroup for the container
  -f, --file string                   Name of the Dockerfile (default: "PATH/Dockerfile")
      --iidfile string                Write the image ID to the file
      --label stringArray             Set metadata for an image
      --load                          Shorthand for "--output=type=docker"
      --metadata-file string          Write build result metadata to the file
      --network string                Set the networking mode for the "RUN" instructions during build (default "default")
      --no-cache                      Do not use cache when building the image
      --no-cache-filter stringArray   Do not cache specified stages
  -o, --output stringArray            Output destination (format: "type=local,dest=path")
      --platform stringArray          Set target platform for build
      --progress string               Set type of progress output ("auto", "plain", "tty"). Use plain to show container output (default "auto")
      --provenance string             Shortand for "--attest=type=provenance"
      --pull                          Always attempt to pull all referenced images
      --push                          Shorthand for "--output=type=registry"
  -q, --quiet                         Suppress the build output and print image ID on success
      --sbom string                   Shorthand for "--attest=type=sbom"
      --secret stringArray            Secret to expose to the build (format: "id=mysecret[,src=/local/secret]")
      --shm-size bytes                Size of "/dev/shm"
      --ssh stringArray               SSH agent socket or keys to expose to the build (format: "default|<id>[=<socket>|<key>[,<key>]]")
  -t, --tag stringArray               Name and optionally a tag (format: "name:tag")
      --target string                 Set the target build stage to build
      --ulimit ulimit                 Ulimit options (default [])
```
Searching this documentation, we can see that the **`--iidfile string` option** (i.e. the **second** option) has the description `Write the image ID to the file`.

### Question 2

#### Question

Run docker with the `python:3.9` image in an interactive mode and the entrypoint of `bash`. Now check the `python` modules that are installed (use `pip list`). How many `python` packages/modules are installed?

#### Answer

By running:
```
docker run -it python:3.9.1 /bin/bash
```
in the terminal, a container instance of the `python:3.9.1` image will be created. Within this container instance, running `pip list` produces the output:
```
Package    Version
---------- -------
pip        21.0.1
setuptools 53.0.0
wheel      0.36.2
```
i.e. there are **three packages** installed within the `python:3.9.1` image. To exit the `python` container, simply press `Ctrl + d`, or type `exit`.

### Intermission: Prepare Postgres Database

Before answering Questions 3 through to 6, we need to set up our Postgres server and ingest the CSV data found at the following two links:
```bash
# Green taxi trip data:
https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
# Taxi zone lookup table:
https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```
The first of these CSVs contains all of the trip data for the 'Green taxis' in New York for 2019, whereas the second CSV contains the numerical ID associated with the name of each geographical zone in New York. Importantly, although the Green taxi trip data records the pick-up and drop-off zones of each trip, these are stored as *numerical IDs*; to get the actual name of the pick-up and/or drop-off zone of a specific trip, we'll need to cross-reference the geographical ID 

Note that the following instructions all assume that your terminal is inside of the `01-intro/homework` directory.

#### Starting Postgres Container

First, let's get our Postgres server up and running. To make it easier to ingest data into this server, we'll first create a `docker network` ; we'll call this network `pg-network`: 
```bash
docker network create pg-network
```
After creating the network for our Postgres server to connect to, let's `run` a Docker container that's running a Postgres server:
```bash
docker run \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```
There are a few things to note about the `docker run` command we've used here:
1. We're creating our container from the `postgres:13` image.
1. We define the environment variables `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` inside of our container by using `-e` flag arguments. More specifically, we set both the username and password of our database to `root` (very secure, I know), and we set the name of the database to `ny_taxi`.
1. By using the `-p` flag we map port `5432` on our local machine to port `5432` in our container.
1. We specify that our container connect to the `pg-network` network we previously created by using the `--network` flag.
1. We use the `--name` flag to give a name to the *container* itself; we'll be utilising the `pg-database` name we've given to this container when we're ingesting data.

Now, let's shift our attention at actually ingesting data into this server.

#### Data Ingestion

To ingest data into our Postgres database, we'll be utilising the `ingest.py` Python script found in the current directory. For the purposes of ensuring that the ingestion process is reproducible, we'll be running `ingest.py` inside of a container; the `Dockerfile` in the current directory defines the image we'll create 'data ingestion containers' from. Although we won't analyse `ingest.py` nor the `Dockerfile` in any real detail here, it's definitely worth spending some time studying these files in order to understand what's going on inside of them. 

To start off the data ingestion process, we'll first open another terminal instance so that we can run commands in the new terminal whilst the Postgres database runs inside of the other terminal.

With our newly-opened terminal, let's build the Docker image that'll we construct data ingestion containers from:
```
docker build -t taxi_data_ingestion .
```
Here, we've used the `-t` flag to name this image `taxi_data_ingestion`. 

Next, we'll `run` a container off our built image to ingest the 'Green Taxi Trip' CSV data:
```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
docker run -it \
    --network=pg-network \
    taxi_data_ingestion \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL} \
    --verbose=True
```
Notice here that we've used the `--network` flag to specify that this data ingestion container should be connected to the same network as our Postgres database: this will allow the data ingestion container to 'communicate' with the Postgres container. Moreover, all of the arguments values provided after the `taxi_data_ingestion \` line are passed to `inget.py` to deal with; here's what each of these arguments specify:
1. `--user` specifies the username we'll use to connect to our Postgres database; we set this value to `root` in order to match the `POSTGRES_USER` environment variable we defined with an `-e` flag when we created our Postgres container.
1.  `--password` specifies the password we'll use to connect to our postgres database; this value is set to `root`, which matches with the `POSTGRES_PASSWORD` environment variable in the Postgres container. 
1. `--host` specifies the name of the machine we're wanting to connect to (i.e. the machine running the database). In this case, we've specified this to be `pg-database`, which is the *name we previously gave to the Postgres container* by using the `--name` flag.
1. `--port` specifies the port number on the machine specified by `--host` that we'll use to connect to the Postgres database; this is set to `5432`, which is the port number that Postgres 'listens to' for inputs by default.
1. `--db` specifies the name of the database we want to connect to; this value is set to `ny_taxi`, which matches with the `POSTGRES_DB` environment variable we defined when we created our Postgres container.
1. `--table_name` sets the name we want to give to the table that will store the ingested data inside of the database; in this case, we'll be naming the table `green_taxi_trips`.
1. Unsurprisingly, `--url` specifies the URL from which the CSV data should be downloaded.
1. `--verbose` specifies that the progress of the ingestion process should be printed back to the user.

To see how each of these argument values are used by `ingest.py` we, once again, suggest that you look over the `ingest.py` code. 

After the 'Green Taxi Trip' data has been ingested (note that this will take *a minute or two* to complete), we'll `run` another container using the same image to ingest the 'Taxi Zone' data:
```bash
URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
docker run -it \
    --network=pg-network \
    taxi_data_ingestion \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=taxi_zones \
    --url=${URL} \
    --verbose=True
```
Notice here that we've set the `--table_name` argument to `taxi_zones` so that we don't overwrite the `green_taxi_trips` table we previously ingested.  Additionally, we've redefined the `URL` environment variable so that it stores the URL of the 'Taxi Zone' CSV data. Since the Taxi Zones data is a lot smaller than the Green Taxi Trip data, this ingestion process should be take only a second or two.

Now that we've ingested both sets of CSV data, all that's left to do is connect to our database so that we can query it.

#### Connecting to Database with `pgcli`

To connect to our Postgres database, we'll use `pgcli`:
```bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```
Here, the `-h` and `-p` are specifing that we connect to the database using the `5432` port on *our local machine* (i.e. `localhost`). This works since the `5432` port of the Postgres container is mapped to the `5432` port on our local machine. Moreover, the `-d` argument specifies that we should connect to the `ny_taxi` database in this container, and the `-u` argument specified that we should login as the user `root`.


After entering our password (i.e. `root`), we can use `\dt` to see a list of tables in our database:
```
+--------+------------------+-------+-------+
| Schema | Name             | Type  | Owner |
|--------+------------------+-------+-------|
| public | green_taxi_trips | table | root  |
| public | taxi_zones       | table | root  |
+--------+------------------+-------+-------+
```
As expected, we see both the `green_taxi_trips` and `taxi_zones` tables we previously ingested.

We're now in a position to answer the remaining questions in Part A of this homework. Before doing so, however, it's probably a good idea to perform some 'sanity checks' to make sure that the ingestion process has been completed correctly.

#### Sanity Check on Data Ingestion

The first obvious thing to check is that we've ingested all of the rows in the CSV files. To perform this check, we'll need to open *yet another* terminal instance; this means we should now have at least three terminals running:
1. The terminal running the Postgres database.
1. The terminal running `pgcli`, which is connect to the database running in the first terminal.
1. The new terminal we've just opened. 
Inside this new terminal instance, we'll download the each CSV using `wget`; since the 'Green Taxi Trip' is zipped, we'll unzip it using `gunzip`:
```bash
# Download and unzip 'Green Taxi Trip' data:
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
gunzip green_tripdata_2019-01.csv.gz
# Download 'Taxi Zones' data:
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```
We can count the number of rows (i.e. lines) in each CSV by using `wc -l`:
```bash
wc -l green_tripdata_2019-01.csv
wc -l taxi+_zone_lookup.csv
```
This prints the following to the terminal:
```
630919 green_tripdata_2019-01.csv
266 taxi+_zone_lookup.csv
```
i.e. the 'Green Taxi Trip' data contains `630919` rows (or `630918` rows if we exclude the column names), whilst the 'Taxi Zone' data contains `266` rows (or `265` rows if we exclude the column names). At this point, we can now close our current terminal instance, and go back to the terminal running `pgcli`.

Let's now compare the number of lines counted by `wc` to the number of rows in each of our ingested Postgres tables. To get the number of rows in the `green_taxi_trips` table, we can execute the query:
```sql
SELECT COUNT(*) FROM public.green_taxi_trips
```
This returns:
```
+--------+
| count  |
|--------|
| 630918 |
+--------+
```
Thankfully, this value agrees with the number of (non-column name) rows counted by `wc`. Similarly, we can use the query:
```sql
SELECT COUNT(*) FROM public.taxi_zones 
```
to count the number of rows in our `taxi_zones` table; this returns:
```
+-------+
| count |
|-------|
| 265   |
+-------+
```
Once again, this agrees with what was returned by `wc`.

Now that we've confirmed that all of the rows in the CSV have been ingested, let's check that the first 10 rows of each table contain sensible values. First, let's check the first ten rows of `green_taxi_trips`:
```sql
SELECT lpep_dropoff_datetime, fare_amount, passenger_count FROM public.green_taxi_trips LIMIT 10
```
Because `green_taxi_trips` contains a lot of columns, we've only queried a small subset of these columns here for the sake brevity. This query returns:
```
+-----------------------+-------------+-----------------+
| lpep_dropoff_datetime | fare_amount | passenger_count |
|-----------------------+-------------+-----------------|
| 2018-12-21 15:18:57   | 3           | 5               |
| 2019-01-01 00:16:32   | 6           | 2               |
| 2019-01-01 00:31:38   | 4.5         | 2               |
| 2019-01-01 01:04:54   | 13.5        | 2               |
| 2019-01-01 00:39:43   | 18          | 1               |
| 2019-01-01 00:19:09   | 6.5         | 1               |
| 2019-01-01 01:00:01   | 13.5        | 1               |
| 2019-01-01 00:30:50   | 16          | 1               |
| 2019-01-01 00:39:46   | 25.5        | 1               |
| 2019-01-01 01:19:02   | 15.5        | 1               |
+-----------------------+-------------+-----------------+
```
Reassuringly, the `lpep_dropoff_datetime` values appear to have been correctly converted to datetime values; additionally, the `fare_amount` and `passenger_count` values also appear to look reasonable.

Let's now check the first 10 columns of the `taxi_zones` table:
```sql
SELECT * FROM public.taxi_zones LIMIT 10
```
This query returns:
```
+-------+------------+---------------+-------------------------+--------------+
| index | LocationID | Borough       | Zone                    | service_zone |
|-------+------------+---------------+-------------------------+--------------|
| 0     | 1          | EWR           | Newark Airport          | EWR          |
| 1     | 2          | Queens        | Jamaica Bay             | Boro Zone    |
| 2     | 3          | Bronx         | Allerton/Pelham Gardens | Boro Zone    |
| 3     | 4          | Manhattan     | Alphabet City           | Yellow Zone  |
| 4     | 5          | Staten Island | Arden Heights           | Boro Zone    |
| 5     | 6          | Staten Island | Arrochar/Fort Wadsworth | Boro Zone    |
| 6     | 7          | Queens        | Astoria                 | Boro Zone    |
| 7     | 8          | Queens        | Astoria Park            | Boro Zone    |
| 8     | 9          | Queens        | Auburndale              | Boro Zone    |
| 9     | 10         | Queens        | Baisley Park            | Boro Zone    |
+-------+------------+---------------+-------------------------+--------------+
```
Once again, nothing looks obviously wrong here.

At this point, we have reason to be relatively confident that both of the CSVs were correctly ingested into the Postgres database. Consequently, we can now move on to answering the remaining questions in Part A of the homework.

### Question 3

#### Question

What was the total number of taxi trips that started **and** finished on `2019-01-15`? Remember that the `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the timestamp format (i.e. `date` and `hour+min+sec`).

#### Answer

Consider the following SQL query:
```sql
SELECT COUNT(*)
FROM public.green_taxi_trips
WHERE (DATE(lpep_pickup_datetime)='2019-01-15' 
AND DATE(lpep_dropoff_datetime)='2019-01-15');
```
This query `COUNT`s the number of rows `WHERE` both the `DATE` of `lpep_pickup_datetime` (i.e. the pick-up date) `AND` the `DATE` of `lpep_dropoff_datetime` (i.e. the drop-off date) equal `2019-01-15`. This query returns:
```
+-------+
| count |
|-------|
| 20530 |
+-------+
```
i.e. there were `20530` taxi trips that both started and finished on `2019-01-15`.

### Question 4

#### Question

On which day did the longest taxi trip in terms of *distance* (not time) occur? Use the pick up time for your calculations (as opposed to the drop-off time).

#### Answer

The following query:
```sql
SELECT DATE(lpep_pickup_datetime)
FROM public.green_taxi_trips
ORDER BY trip_distance DESC
LIMIT 1;
```
first `ORDERS` the rows in `green_taxi_trips` `BY trip_distance` in `DESC`ending order, and then returns the `DATE(lpep_pickup_datetime)` (i.e. pick-up date) of the first row (i.e. `LIMIT 1`) in this ordered list of rows. This query returns:
```
+------------+
| date       |
|------------|
| 2019-01-15 |
+------------+
```
i.e. the longest taxi drip in terms of distance occurred on `2019-01-15`.

### Question 5

#### Question

On 2019-01-01, how many trips had 2 passengers, and how many trips had 3 passengers?

#### Answer

To answer this question, we'll be using [*Common Table Expressions* (CTEs)](https://learnsql.com/blog/what-is-common-table-expression/). Consider the following query:
```sql
WITH first_day_trips AS (
    SELECT passenger_count FROM public.green_taxi_trips
    WHERE DATE(lpep_pickup_datetime) = '2019-01-01' 
    OR DATE(lpep_dropoff_datetime) = '2019-01-01'
),    
    two_passenger_trips AS (
    SELECT COUNT(*) as count FROM first_day_trips
    WHERE passenger_count = 2
),
    three_passenger_trips AS (
    SELECT COUNT(*) as count FROM first_day_trips
    WHERE passenger_count = 3
)
SELECT two_passenger_trips.count AS two_passengers, 
       three_passenger_trips.count AS three_passengers
FROM two_passenger_trips, three_passenger_trips;
```
To understand what's going on here, let's 'disect' this query in chunks. First, let's look at:
```sql
WITH first_day_trips AS (
    SELECT passenger_count FROM public.green_taxi_trips
    WHERE DATE(lpep_pickup_datetime) = '2019-01-01' 
    OR DATE(lpep_dropoff_datetime) = '2019-01-01'
), 
```
This defines a common table called `first_day_trips`, which contains the `passenger_count` of those trips where the `DATE(lpep_pickup_datetime)` (i.e. pick-up date) `OR` `DATE(lpep_pickup_datetime)` (i.e. the drop-off date) equal `2019-01-01`. In effect, `first_day_trips` stores the `passenger_count` of all those trips that either started *or* ended on the first day of `2019`.

Next, let's consider the chunk:
```sql
    two_passenger_trips AS (
    SELECT COUNT(*) AS count FROM first_day_trips
    WHERE passenger_count = 2
),
```
This defines the common table `two_passenger_trips`, which contains the number of rows (i.e. `COUNT(*)`) in the common table `first_day_trips` `WHERE` `passenger_count = 2`; this value is stored `AS` a column called `count`. In effect, `two_passenger_trips` is a table that contains a single value, that being the number of two-passenger trips that occured on the first day of `2019`. Similarly, the common table `three_passenger_trips` defined by the chunk:
```
    three_passenger_trips AS (
    SELECT COUNT(*) as count FROM first_day_trips
    WHERE passenger_count = 3
)
```
simply contains the number of three-passenger trips that occured on the first day of `2019`.

Finally, with the `two_passenger_trips` and `three_passenger_trips` common tables defind, we can now understand the final part of this query:
```sql
SELECT two_passenger_trips.count AS two_passengers, 
       three_passenger_trips.count AS three_passengers
FROM two_passenger_trips, three_passenger_trips;
```
This  simply returns the `count` values in `two_passenger_trips` and `three_passenger_trips` (i.e. the only value stored in these tables); these values are placed in columns called  `two_passengers` and `three_passengers` respectively.

Running this query returns the following result:
```
+----------------+------------------+
| two_passengers | three_passengers |
|----------------+------------------|
| 1282           | 254              |
+----------------+------------------+
```
i.e. `1282` two-passenger trips took place on `01-01-2019`, whilst `254` three-passenger trips took place on `01-01-2019`.

### Question 6

#### Question

Among all of the passengers picked up in the Astoria Zone, what was the drop-off zone of the trip that gave the largest tip (i.e. 'tip' as in 'monetary tip', *not* 'trip')? Note that we want the name of the zone, not the numerical id.

#### Answer

To answer this question, we have to use *both* the `green_taxi_trips` table, as well as the `taxi_zones` table. This is because the zone of each trip recorded in `green_taxi_trips` is denoted using a numerical ID rather than with a string; the numerical ID associated with each taxi zone name, however, can be found in `taxi_zones`. 

With this in mind, consider the following query:
```sql
WITH astoria_zone AS (
    SELECT "LocationID" AS id FROM public.taxi_zones
    WHERE "Zone" = 'Astoria'
),
largest_tip_id AS (
    SELECT "DOLocationID" AS id FROM public.green_taxi_trips
    WHERE "PULocationID" = (SELECT id FROM astoria_zone)
    ORDER BY tip_amount DESC
    LIMIT 1
),
largest_tip_name AS (
    SELECT "Zone" AS largest_tip_location FROM public.taxi_zones
    WHERE "LocationID" = (SELECT id FROM largest_tip_id)
)
SELECT largest_tip_location FROM largest_tip_name;
```
Like before, let's interpret this query piece-by-piece. The first part of this query
```sql
WITH astoria_zone AS (
    SELECT LocationID AS id FROM public.taxi_zones
    WHERE "zone" = 'Astoria'
),
```
simply extracts the numerical `LocationID` of the `Astoria` zone from the `taxi_zones` table. There are two subtleties to note here:
1. For clarity, the `"zone"` in `WHERE "zone" = '"Astoria"'` is enclosed within double-quotes, since `ZONE` is a keyword in SQL. Although the query still runs correctly when these double-quotes are omitted, the syntax highlighting incorrectly recognises `zone` as a keyword, rather than as a column name.
2. When ingesting the `taxi_zones` data, we didn't strip the 

Next, we use the numerical ID stored in the `astoria_zone` to create another common table using `green_taxi_trips`:
```sql
largest_tip_id AS (
    SELECT DOLocationID AS id FROM public.green_taxi_trips
    WHERE PULocationID = (SELECT id FROM astoria_zone)
    ORDER BY tip_amount DESC
    LIMIT 1
),
```
In this case, first taking only those rows `WHERE` `PULocationID` (i.e. the numerical ID of the pick-up location) equals the numerical ID of the Astoria zone, which we've stored in the `astoria_zone` common table; the ID value stored in `astoria_zone` is accessed using the sub-query `(SELECT id FROM astoria_zone)`. The returned rows are then `ORDERED` in `DESC`ending order by `tip_amount`, and the `DOLocationID` (i.e. numerical ID of the drop-off zone) of the first row (i.e. `LIMIT 1`) is taken. As a result of all this, we now have the numerical ID of the drop-off zone of the trip that gave the largest monetary tip among all those trips that begun in the Astoria zone.

To convert the numerical ID we're now storing in the common table `largest_tip_id`, we have to use the `taxi_zones` table again:
```sql
largest_tip_name AS (
    SELECT "zone" AS largest_tip_location FROM public.taxi_zones
    WHERE LocationID = (SELECT id FROM largest_tip_id)
)
```
In this chunk, we're extracting the name of the `zone` that has the same numerical ID as the drop-off location ID we've stored in `largest_tip_id`, which we access here by using the sub-query `(SELECT id FROM largest_tip_id)`. The extract `zone` name is stored in the column `largest_tip_location`.



Running this query returns the result:
```
+-------------------------------+
| largest_tip_location          |
|-------------------------------|
| Long Island City/Queens Plaza |
+-------------------------------+
```
i.e. the drop-offzone of the largest tipping trip that started in the Astoria Zone was the `Long Island City/Queens Plaza` zone.

## Part B

Part B of the homework can be found [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_terraform/homework.md) within the Data Engineering Zoomcamp repository.

### Question 1

#### Question

Copy the `main.tf` and `variables.tf` Terraform files from [this directory](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform) within the Data Engineering Zoomcamp Github repository. For convenience, these files have already been copied into this repository, and can be found within the `01-intro/homework/terraform` directory. Note that you may wish to change the value of the `"region"` variable within `variables.tf` to a GCP region which is closer to your current geographical location. 

After creating a project on GCP and granting the correct permissions, run `terraform apply` to create the GCP resources defined in the `main.tf` file (i.e. a Google Cloud Storage Bucket and a BigQuery Table). 

Copy down the output that is printed to the terminal after running `terraform apply`.


#### Answer


Upon creating a GCP project with the ID `clear-nebula-375807`, setting the value of the `GOOGLE_APPLICATION_CREDENTIALS` environment variable,  and calling `terraform apply`, the following output is printed:

```
var.project
  Your GCP Project ID

  Enter a value: clear-nebula-375807


Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "trips_data_all"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + labels                     = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "australia-southeast1"
      + project                    = "clear-nebula-375807"
      + self_link                  = (known after apply)

      + access {
          + domain         = (known after apply)
          + group_by_email = (known after apply)
          + role           = (known after apply)
          + special_group  = (known after apply)
          + user_by_email  = (known after apply)

          + dataset {
              + target_types = (known after apply)

              + dataset {
                  + dataset_id = (known after apply)
                  + project_id = (known after apply)
                }
            }

          + routine {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + routine_id = (known after apply)
            }

          + view {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + table_id   = (known after apply)
            }
        }
    }

  # google_storage_bucket.data-lake-bucket will be created
  + resource "google_storage_bucket" "data-lake-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "AUSTRALIA-SOUTHEAST1"
      + name                        = "dtc_data_lake_clear-nebula-375807"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }

          + condition {
              + age                   = 30
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }

      + website {
          + main_page_suffix = (known after apply)
          + not_found_page   = (known after apply)
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_storage_bucket.data-lake-bucket: Creation complete after 2s [id=dtc_data_lake_clear-nebula-375807]
google_bigquery_dataset.dataset: Creation complete after 3s [id=projects/clear-nebula-375807/datasets/trips_data_all]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```
Since we're not going to be doing anything with these resources (at least not for Week 1), it's probably a good idea to destroy them using `terraform destroy`.  Upon calling `terraform destroy`, the following is printed:
```
var.project
  Your GCP Project ID

  Enter a value: clear-nebula-375807

google_storage_bucket.data-lake-bucket: Refreshing state... [id=dtc_data_lake_clear-nebula-375807]
google_bigquery_dataset.dataset: Refreshing state... [id=projects/clear-nebula-375807/datasets/trips_data_all]

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  - destroy

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be destroyed
  - resource "google_bigquery_dataset" "dataset" {
      - creation_time                   = 1674730624914 -> null
      - dataset_id                      = "trips_data_all" -> null
      - default_partition_expiration_ms = 0 -> null
      - default_table_expiration_ms     = 0 -> null
      - delete_contents_on_destroy      = false -> null
      - etag                            = "fJTCmVOZq00367r36Au6XA==" -> null
      - id                              = "projects/clear-nebula-375807/datasets/trips_data_all" -> null
      - labels                          = {} -> null
      - last_modified_time              = 1674730624914 -> null
      - location                        = "australia-southeast1" -> null
      - project                         = "clear-nebula-375807" -> null
      - self_link                       = "https://bigquery.googleapis.com/bigquery/v2/projects/clear-nebula-375807/datasets/trips_data_all" -> null

      - access {
          - role          = "OWNER" -> null
          - user_by_email = "matt.a.bilton@gmail.com" -> null
        }
      - access {
          - role          = "OWNER" -> null
          - special_group = "projectOwners" -> null
        }
      - access {
          - role          = "READER" -> null
          - special_group = "projectReaders" -> null
        }
      - access {
          - role          = "WRITER" -> null
          - special_group = "projectWriters" -> null
        }
    }

  # google_storage_bucket.data-lake-bucket will be destroyed
  - resource "google_storage_bucket" "data-lake-bucket" {
      - default_event_based_hold    = false -> null
      - force_destroy               = true -> null
      - id                          = "dtc_data_lake_clear-nebula-375807" -> null
      - labels                      = {} -> null
      - location                    = "AUSTRALIA-SOUTHEAST1" -> null
      - name                        = "dtc_data_lake_clear-nebula-375807" -> null
      - project                     = "clear-nebula-375807" -> null
      - public_access_prevention    = "inherited" -> null
      - requester_pays              = false -> null
      - self_link                   = "https://www.googleapis.com/storage/v1/b/dtc_data_lake_clear-nebula-375807" -> null
      - storage_class               = "STANDARD" -> null
      - uniform_bucket_level_access = true -> null
      - url                         = "gs://dtc_data_lake_clear-nebula-375807" -> null

      - lifecycle_rule {
          - action {
              - type = "Delete" -> null
            }

          - condition {
              - age                        = 30 -> null
              - days_since_custom_time     = 0 -> null
              - days_since_noncurrent_time = 0 -> null
              - matches_prefix             = [] -> null
              - matches_storage_class      = [] -> null
              - matches_suffix             = [] -> null
              - num_newer_versions         = 0 -> null
              - with_state                 = "ANY" -> null
            }
        }

      - versioning {
          - enabled = true -> null
        }
    }

Plan: 0 to add, 0 to change, 2 to destroy.

Do you really want to destroy all resources?
  Terraform will destroy all your managed infrastructure, as shown above.
  There is no undo. Only 'yes' will be accepted to confirm.

  Enter a value: yes

google_storage_bucket.data-lake-bucket: Destroying... [id=dtc_data_lake_clear-nebula-375807]
google_bigquery_dataset.dataset: Destroying... [id=projects/clear-nebula-375807/datasets/trips_data_all]
google_storage_bucket.data-lake-bucket: Destruction complete after 1s
google_bigquery_dataset.dataset: Destruction complete after 2s

Destroy complete! Resources: 2 destroyed.
```

## Extras

This point marks the end of my answers to the Week 1 homework questions. Although the computational tools we used to answer these questions worked as intended, there's definitely room for imrovement. In particular, the ingestion process we utilised here was, by far, the slowest part of our workflow, with our Python ingestion script taking between one to two minutes to ingest the 'Green Taxi Data' CSV file. With this in mind, `03-extras.md` goes over how the ingestion process can be *significantly* sped up by replacing the `pandas`'s `to_sql` method called within `ingest.py` with `psycopg2`'s `copy_from` method.

