
set -e

TAXI_TYPE=$1 # this will let you delcare variable in bash
YEAR=$2 # 2021

URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"
FILE_FORMAT_PARQUET="parquet"
FILE_FORMAT_CSV="csv"

for MONTH in {1..12}; do # for loop in C?
    # create month format in 01 02
    FMONTH=`printf "%02d" ${MONTH}` 
    
    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.${FILE_FORMAT_PARQUET}"

    # create local data storage
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.${FILE_FORMAT_PARQUET}"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"
    LOCAL_FILE_CSV="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.${FILE_FORMAT_CSV}"
    LOCAL_PATH_CSV="${LOCAL_PREFIX}/${LOCAL_FILE_CSV}"

    echo "downloading ${URL} to ${LOCAL_PATH}"
    mkdir -p ${LOCAL_PREFIX} # create directory
    wget ${URL} -O ${LOCAL_PATH} # download data with specific local path

    echo "Transform to csv"
    echo "${LOCAL_PATH} --> ${LOCAL_PATH_CSV}"
    python transform.py --parquet ${LOCAL_PATH} --csv ${LOCAL_PATH_CSV}

    echo "Deleting parquet format"
    rm ${LOCAL_PATH}

    echo "compressing ${LOCAL_PATH_CSV}"
    gzip ${LOCAL_PATH_CSV} # zip file
done