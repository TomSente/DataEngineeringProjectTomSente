# Data Engineering Project

This project contains two Airflow-orchestrated ETL pipelines:

- A batch pipeline for taxi trip data in [BatchProcessing](BatchProcessing)
- A near-realtime pipeline for ship data in [RealtimeProcessing](RealtimeProcessing)

Both pipelines follow the same ETL pattern:

- Extract data from a local file or API
- Validate the input
- Transform the raw data into analysis-ready features
- Write the processed output locally

## Project Structure

- [airflow-docker](airflow-docker) contains the Airflow stack, DAGs, config, and logs
- [BatchProcessing](BatchProcessing) contains the taxi ETL code
- [RealtimeProcessing](RealtimeProcessing) contains the ship ETL code

## Requirements

- Docker Desktop installed and running
- Docker Compose available
- PowerShell or another terminal

The Airflow container uses the settings in [airflow-docker/.env](airflow-docker/.env):

- `AIRFLOW_UID=50000`
- `_PIP_ADDITIONAL_REQUIREMENTS=pandas pyarrow requests azure-storage-blob openpyxl`

## How To Launch The Project

1. Open a terminal in the [airflow-docker](airflow-docker) folder.
2. Initialize Airflow the first time:

```powershell
docker compose up airflow-init
```

3. Start the full Airflow stack:

```powershell
docker compose up -d
```

4. Open the Airflow UI in your browser:

```text
http://localhost:8080
```

5. Log in with the default credentials if your local setup still uses the compose defaults:

- Username: `airflow`
- Password: `airflow`

## Running The Pipelines

In the Airflow UI, you should see these DAGs:

- `taxi_batch_pipeline`
- `ship_realtime_pipeline`

Trigger `taxi_batch_pipeline` to process the taxi parquet file from [BatchProcessing/input](BatchProcessing/input) and write the processed result to [BatchProcessing/output](BatchProcessing/output).

Trigger `ship_realtime_pipeline` to start monitoring [RealtimeProcessing/input](RealtimeProcessing/input) folder to process it, and write the output to [RealtimeProcessing/output](RealtimeProcessing/output).

## Useful Notes

- The DAGs mount the repository root into the Airflow container, so Python modules can be imported correctly.
- The ship pipeline fetches API data inside the DAG task, not only when run locally.
- Logs are stored under [airflow-docker/logs](airflow-docker/logs) and are ignored by git.

## Stopping The Stack

```powershell
docker compose down
```

If you also want to remove the database volumes, use:

```powershell
docker compose down -v
```

## Troubleshooting

- If Airflow does not start, make sure Docker Desktop has enough CPU and memory assigned.
- If a DAG cannot import the pipeline modules, confirm you launched the stack from the [airflow-docker](airflow-docker) folder.
- If you changed the code and the UI does not reflect it, restart the stack with `docker compose down` and `docker compose up -d`.
