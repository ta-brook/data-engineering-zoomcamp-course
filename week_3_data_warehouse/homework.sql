-- Create External table for FHV
CREATE OR REPLACE EXTERNAL TABLE `tensile-ethos-349916.trips_data_all.external_fhv`
OPTIONS (
    format='parquet',
    uris=['gs://dtc_data_lake_tensile-ethos-349916/raw/fhv_tripdata/2019/2019/fhv_tripdata_2019*.parquet']
);

-- Create table in BQ
CREATE OR REPLACE TABLE `tensile-ethos-349916.trips_data_all.fhv_non_partitioned`
AS 
SELECT * FROM `tensile-ethos-349916.trips_data_all.external_fhv`;

-- 1
SELECT COUNT(1) FROM `tensile-ethos-349916.trips_data_all.fhv_non_partitioned`;


-- 2
SELECT 
    COUNT(DISTINCT dispatching_base_num)
FROM `tensile-ethos-349916.trips_data_all.fhv_non_partitioned`;


-- Create partition on dropoff_datetime and cluster on dispatching_base_num
CREATE OR REPLACE TABLE `tensile-ethos-349916.trips_data_all.fhv_partitioned`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num 
    AS
SELECT * FROM `tensile-ethos-349916.trips_data_all.fhv_non_partitioned`;

-- 3
SELECT COUNT(*) FROM 
`tensile-ethos-349916.trips_data_all.fhv_partitioned`
WHERE DATE(dropoff_datetime) BETWEEN DATE '2019-01-01' AND DATE '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');