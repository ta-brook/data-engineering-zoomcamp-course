# dbt with BigQuery on Docker

This is a quick guide on how to setup dbt with BigQuery on Docker.

**Note:** You will need your authentication json key file for this method to work. You can use oauth alternatively.

- Create a directory with the name of your choosing. which is week_4_analytics_eng_n_dbt in my repo.
  ```
  mkdir <dir-name>
  ```
- cd into the directory
  ```
  cd <dir-name>
  ```
- Copy this [Dockerfile](Dockerfile) in your directory borrowed from the official dbt git [here](https://github.com/dbt-labs/dbt-core/blob/main/docker/Dockerfile)
- *Note: there are an error from official file that specified python version to 3.10 and there is a (bug)[https://github.com/dbt-labs/dbt-core/issues/4560] on it. Anyway, I fix it with change to 3.9 version as suggested in github issue above.*

- Create `docker-compose.yaml` [file](docker-compose.yaml).
  ```yaml
  version: '3'
    services:
      de-dbt-zoomcamp:
        build:
          context: .
          target: dbt-bigquery
        image: dbt/bigquery
        volumes:
          - .:/usr/app
          - ~/.dbt/:/root/.dbt/
          - ~/.google/credentials/google_credentials.json:/.google/credentials/google_credentials.json
        network_mode: host
  ```
  -   Name the service as you deem right or `de-dbt-zoomcamp`.
  -   Use the `Dockerfile` in the current directory to build the image by passing `.` in the context.
  -   `target` specifies that we want to install the `dbt-bigquery` plugin in addition to `dbt-core`.
  -  Mount 3 volumes -
     - for persisting dbt data
     - path to the dbt `profiles.yml`
     - path to the `google_credentials.json` file which should be in the `~/.google/credentials/` path

- Create `profiles.yml` file in `~/.dbt/` in your local machine or add the following code in your existing `profiles.yml` - 
  - *Note: I'm not really sure about this step because when I run `dbt init` via docker it's wont read anything from `profiles.yml` but ask everything from me and create a new one instead.*
  - by doing this going to home directory and create .dbt folder.
  ```
  cd ~
  mkdir .dbt
  cd .dbt
  code .
  ```

  ```yaml
  bq-dbt-workshop:
    outputs:
      dev:
        dataset: dbt_book_lyn
        fixed_retries: 1
        keyfile: /.google/credentials/google_credentials.json
        location: US
        method: service-account
        priority: interactive
        project: <project-id>
        threads: 4
        timeout_seconds: 300
        type: bigquery
    target: dev
  ```
  - Name the profile. `bq-dbt-workshop` in my case. This will be used in the `dbt_project.yml` file to refer and initiate dbt.
  - Replace with your `dataset`, `location` (my GCS bucket is in `EU` region, change to `US` if needed), `project` values.
- Run the following commands -
  - ```bash 
    docker compose build 
    ```
  - ```bash 
    docker compose run de-dbt-zoomcamp init
    ``` 
    - **Note:** We are essentially running `dbt init` above because the `ENTRYPOINT` in the [Dockerfile](Dockerfile) is `['dbt']`.
    - Input the required values. Project name will be `taxi_rides_ny`
    - This should create `dbt/taxi_rides_ny/` and you should see `dbt_project.yml` in there.
    - In `dbt_project.yml`, replace `profile: 'taxi_rides_ny'` with `profile: 'bq-dbt-workshop'` as we have a profile with the later name in our `profiles.yml`
  - ```bash
    docker compose run --workdir="//usr/app/dbt/taxi_rides_ny" de-dbt-zoomcamp debug
     ``` 
    - to test your connection. This should output `All checks passed!` in the end.
    - **Note:** The automatic path conversion in Git Bash will cause the commands to fail with `--workdir` flag. It can be fixed by prefixing the path with `//` as is done above. The solution was found [here](https://github.com/docker/cli/issues/2204#issuecomment-638993192).
    - Also, we change the working directory to the dbt project because the `dbt_project.yml` file should be in the current directory. Else it will throw `1 check failed: Could not load dbt_project.yml`

### Run any command
```
docker compose run \
  --workdir="//usr/app/dbt/taxi_rides_ny" \
  de-dbt-zoomcamp \
  <command here>
```


### Run model
```
docker compose run \
  --workdir="//usr/app/dbt/taxi_rides_ny" \
  de-dbt-zoomcamp \
  run
```

### Port listen
```
netstat -na | find "your_port"
```

### dbt Docs

Generate your project's documentation website first - 
```bash
docker compose run \
  --workdir="//usr/app/dbt/taxi_rides_ny" \
  dbt-de-zoomcamp \
  docs generate
```

Serve your docs -

```bash
docker compose run \
  --workdir="//usr/app/dbt/taxi_rides_ny" \
  dbt-de-zoomcamp \
  docs serve 
```

**Note:** If the above commands seem inconvenient, you can change the `entrypoint` to `bash` using the below command and run commands normally like `dbt run` 
```bash
docker compose run --entrypoint='bash' dbt-de-zoomcamp
```


### Ref
- [de-zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering/docker_setup)
- [ankurchavda note](https://github.com/ankurchavda/data-engineering-zoomcamp/blob/main/4_dbt/docker-setup.md)