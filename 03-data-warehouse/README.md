### Big Query Setup
```sql
-- Create External Table for Yellow Taxi Trips
CREATE OR REPLACE EXTERNAL TABLE `de_zoomcamp.yellow_taxi_trips_raw_2024_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-zoomcamp-tychong/raw/yellow-taxi/2024/*.parquet']
);

-- Create Regular Table for Yellow Taxi Trips
CREATE OR REPLACE TABLE `de_zoomcamp.yellow_taxi_trips_raw_2024`
AS
SELECT * FROM `de_zoomcamp.yellow_taxi_trips_raw_2024_ext`;
```


### Question 1
Question 1: What is count of records for the 2024 Yellow Taxi Data?
```sql
SELECT COUNT(*) AS total_records
FROM de_zoomcamp.yellow_taxi_trips_raw_2024;
```

Answer: `20,332,093`

### Question 2
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.


```sql
-- Query from external table
SELECT COUNT(DISTINCT PULocationID) as total_pickup_location
FROM de_zoomcamp.yellow_taxi_trips_raw_2024_ext;

-- Query from regular table
SELECT COUNT(DISTINCT PULocationID) as total_pickup_location
FROM de_zoomcamp.yellow_taxi_trips_raw_2024;
```
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

Ansewr: `
0 MB for the External Table and 155.12 MB for the Materialized Table`

### Question 3
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

```sql
-- 155.12 MB
SELECT PULocationID
FROM de_zoomcamp.yellow_taxi_trips_raw_2024;

-- 310.24 MB
SELECT PULocationID, DOLocationID
FROM de_zoomcamp.yellow_taxi_trips_raw_2024;
```

Answer: `BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
`

### Question 4
How many records have a fare_amount of 0?

```sql
SELECT COUNT(*) as total_records_with_fare
FROM de_zoomcamp.yellow_taxi_trips_raw_2024
WHERE fare_amount = 0;
```

Answer: `8,333`

### Question 5

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

```sql
CREATE OR REPLACE TABLE `de_zoomcamp.yellow_taxi_trips_raw_2024_partitioned_clustered`
PARTITION BY TIMESTAMP_TRUNC(tpep_dropoff_datetime, DAY)
CLUSTER BY (VendorID)
AS
(SELECT * FROM `de_zoomcamp.yellow_taxi_trips_raw_2024_ext`);
```

Answer: `Partition by tpep_dropoff_datetime and Cluster on VendorID`

### Question 6
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

```sql
-- 310.24 MB
SELECT DISTINCT(VendorID) as unique_vendor
FROM de_zoomcamp.yellow_taxi_trips_raw_2024
WHERE tpep_dropoff_datetime >= '2024-03-01'
AND tpep_dropoff_datetime < '2024-03-16';

-- 26.84 MB
SELECT DISTINCT(VendorID) as unique_vendor
FROM de_zoomcamp.yellow_taxi_trips_raw_2024_partitioned_clustered
WHERE tpep_dropoff_datetime >= '2024-03-01'
AND tpep_dropoff_datetime < '2024-03-16';
```

Answer: `310.24 MB for non-partitioned table and 26.84 MB for the partitioned table`

### Question 7
Where is the data stored in the External Table you created?

Answer: `GCP Bucket`

### Question 8
It is best practice in Big Query to always cluster your data:

Answer: `False`
