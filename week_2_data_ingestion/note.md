airflow clear [-s START_DATE] [-e END_DATE] --only_failed dag_id

airflow clear 2019-01-01 -e 2020-01-01 --only_failed data_ingestion_green_taxi_dag

note: wont work anymore on my 2.2.3 airflow