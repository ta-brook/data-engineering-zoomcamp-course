- `docker build -t test:pandas .`
- `winpty docker run -it test:pandas 2022`

# docker

winpty docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v c://Users/tanaw/Documents/GitHub/data-engineering-zoomcamp-course/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13

winpty docker run -it -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v c://Users/tanaw/Documents/GitHub/data-engineering-zoomcamp-course/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 postgres:13

# install pgcli
pip install pgcli
pgcli -h localhost -p 5432 -u root -d ny_taxi

# install wget
- install wget on window
https://builtvisible.com/download-your-website-with-wget/
- install wget on git bash
https://gist.github.com/evanwill/0207876c3243bbb6863e65ec5dc3f058#wget

# datasets
taxi data
`wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.parquet`

[Optional] transform parquet to csv (13/5/2022 there are only parquet file available to download from taxi dataset)
these `pandas pyarrow fastparquet` are required and then,
`parq = pd.read_parquet('yellow_tripdata_2021-01.parquet')`
`parq.to_csv('yellow_tripdata_2021-01.csv')`

# docker postgres
winpty docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4


# create docker network
docker network create pg-network

winpty docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v c://Users/tanaw/Documents/GitHub/\data-engineering-zoomcamp-course/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13

# pgadmin
winpty docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pgadmin-2 \
    dpage/pgadmin4

# Create scripts
noted: password should be type in terminal (maybe in env file)

URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.parquet"

python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL} 

# dockerize above pipeline
docker build -t taxi_ingest:v001 .

URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.parquet"

winpty docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
        --user=root \
        --password=root \
        --host=pg-database \
        --port=5432 \
        --db=ny_taxi \
        --table_name=yellow_taxi_trips \
        --url=${URL} 

# docker-compose

services:
  pgdatabase:
    image: postgres:13
    environment:
      POSTGRES_USER: root
      POSTGRES_PASSWORD: root
      POSTGRES_DB: ny_taxi
    volumes:
      - "./ny-taxi-volume:/var/lib/postgresql/data:rw"
    ports:
      - "5432:5432"
  pgadmin:
    image: dpage/pgadmin4
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@admin.com
      PGADMIN_DEFAULT_PASSWORD: root
    ports:
      - "8080:80"

- Error response from daemon: i/o timeout fixed via this https://github.com/docker/for-win/issues/4413