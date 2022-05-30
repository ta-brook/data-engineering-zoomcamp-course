# Setup 

https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

## Setup env

```
cd ~ && mkdir -p ~/.google/credentials/
mv <path/to/your/service-account-authkeys>.json ~/.google/credentials/google_credentials.json
```

## Airflow setup

1. 

`curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'`

2.

```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

3. 