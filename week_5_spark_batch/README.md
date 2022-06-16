## Spark and batch processing

- Installation 

[window](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/windows.md)
[Pyspark](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/pyspark.md)

*note: don't forget to add permanent path to your path environment*

[bug and solution I found so far](https://stackoverflow.com/questions/53161939/pyspark-error-does-not-exist-in-the-jvm-error-when-initializing-sparkcontext)

## Path config

- [path reg_sz](https://stackoverflow.com/a/58090669/16172471)

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

## Createing local spark cluster

- spark cluster stand alone [mode](https://spark.apache.org/docs/latest/spark-standalone.html)
- yes, of course [here](https://stackoverflow.com/a/49851757/16172471) a solution for run spark-master.sh

### to run cluster on window

`Note: The launch scripts do not currently support Windows. To run a Spark cluster on Windows, start the master and workers by hand.`

1. go to `%SPARK_HOME%/bin` folder in a command prompt

2. Run `spark-class org.apache.spark.deploy.master.Master` to run the master. This will give you a URL of the form `spark://ip:port`

3. Run `spark-class org.apache.spark.deploy.worker.Worker spark://ip:port` to run the worker. Make sure you use the URL you obtained in step 2.

4. Run `spark-shell --master spark://ip:port` to connect an application to the newly created cluster.

5. change spark connecter in your code to `spark://ip:port` example below:

```
spark = SparkSession.builder \
    .master("spark://ip:port") \
    .appName('test') \
    .getOrCreate()
```

*note: you may need to run this on `cmd`m somehow it not working on my `git bash`*


## to use the py script with args

```bash
python 06_spark_sql.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020
```

### to use with spark-submit (more professional)

```bash
URL="spark://ip:port"

$SPARK_HOME/bin/spark-submit.cmd \
    --master="${URL}" \
    06_spark_sql.py \
        --input_green=data/pq/green/2020/*/ \
        --input_yellow=data/pq/yellow/2020/*/ \
        --output=data/report-2020
```

## dataproc

1. create cluster on gcp

2. upload file to gcs or whatever path on gcp

3. run the script via data proc with added parameter etc.

```
spark-submit.cmd \
    --master="${URL}" \
    06_spark_sql.py \
        --input_green=gs://dtc_data_lake_tensile-ethos-349916/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_tensile-ethos-349916/pq/yellow/2020/*/ \
        --output=gs://dtc_data_lake_tensile-ethos-349916/report-2020/
```

4. (alternative) run dataproc via google sdk

```
gcloud dataproc jobs submit pyspark \
    --cluster=cluster-6597 \
    --region=asia-southeast1 \
    gs://dtc_data_lake_tensile-ethos-349916/code/06_spark_sql.py \
    -- \ 
        --input_green=gs://dtc_data_lake_tensile-ethos-349916/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_tensile-ethos-349916/pq/yellow/2020/*/ \
        --output=gs://dtc_data_lake_tensile-ethos-349916/report-2020/
```

- in case above command not work the bash below work

`gcloud dataproc jobs submit pyspark --cluster=cluster-6597 --region=asia-southeast1 gs://dtc_data_lake_tensile-ethos-349916/code/06_spark_sql.py -- --input_green=gs://dtc_data_lake_tensile-ethos-349916/pq/green/2020/*/ --input_yellow=gs://dtc_data_lake_tensile-ethos-349916/pq/yellow/2020/*/ --output=gs://dtc_data_lake_tensile-ethos-349916/report-2020/`

*note: `--` stand for configuration ended*.

- sdk example:

```bash
gcloud dataproc jobs submit <job-command> \
    --cluster=<cluster-name> \
    --region=<region> \
    gs://<location of script>
    -- \
        -- args=<your-param-value>
```