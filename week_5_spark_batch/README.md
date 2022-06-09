## Spark and batch processing

- Installation 

[window](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/windows.md)
[Pyspark](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/pyspark.md)

*note: don't forget to add permanent path to your path environment*

[bug and solution I found so far](https://stackoverflow.com/questions/53161939/pyspark-error-does-not-exist-in-the-jvm-error-when-initializing-sparkcontext)

## Magic command for this naughty lib

```python
import findspark
findspark.init()
```

## Bash command used in this lesson

- `wc` 
word count the result composed of lines, words, characters and filename
    - add `-l` to get only lines number
    - add `-w` to get only words number
    - add `-m` to get only characters number
    
- `head`
show head data from tables
    - add `-n <number>` for specific number of head rows
    - add `> <filename>` for cp data to new file e.g., `head -n 112 fhv.csv > head_112.csv`

- normal fhv
StructType(
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropOff_datetime', types.TimestampType(), True),
    types.StructField('PUlocationID', types.IntegerType(), True),
    types.StructField('DOlocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)
)

## Action vs Transformation

1. Transformation -- lazy (not executed immediately)
    - Selecting
    - Filtering
    - Joins
    - Group by
    - ...

2. Action -- eager (executed immediately)
    - show, take, head
    - Write
    - ...


## Download data with .sh

yellow: https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.parquet
green: https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2021-01.parquet

- green data

types.StructType([
    types.StructField("VendorID", types.LongType(), True),
    types.StructField("lpep_pickup_datetime", types.TimestampType(), True),
    types.StructField("lpep_dropoff_datetime", types.TimestampType(), True),
    types.StructField("store_and_fwd_flag", types.StringType(), True),
    types.StructField("RatecodeID", types.IntegerType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("passenger_count", types.IntegerType(), True),
    types.StructField("trip_distance", types.DoubleType(), True),
    types.StructField("fare_amount", types.DoubleType(), True),
    types.StructField("extra", types.DoubleType(), True),
    types.StructField("mta_tax", types.DoubleType(), True),
    types.StructField("tip_amount", types.DoubleType(), True),
    types.StructField("tolls_amount", types.DoubleType(), True),
    types.StructField("ehail_fee", types.DoubleType(), True),
    types.StructField("improvement_surcharge", types.DoubleType(), True),
    types.StructField("total_amount", types.DoubleType(), True),
    types.StructField("payment_type", types.LongType(), True),
    types.StructField("trip_type", types.LongType(), True),
    types.StructField("congestion_surcharge", types.DoubleType(), True),
])


