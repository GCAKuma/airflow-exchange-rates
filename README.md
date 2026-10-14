# Airflow Exchange Rates

A local Apache Airflow project that fetches daily USD exchange rates, keeps the currencies used by the example, and stores the results in both JSON and SQLite. The project runs Airflow with Docker Compose, so no local Python or Airflow installation is required.

## What the pipeline does

The `exchange_rate_pipeline` DAG runs once per day with `catchup=False`:

1. **Extract** calls the public ExchangeRate API endpoint `https://open.er-api.com/v6/latest/USD` and saves the complete response to `data/raw_rates.json`.
2. **Transform** selects `USD`, `LKR`, `EUR`, and `GBP`, adds the current UTC date, and writes the normalized rows to `data/clean_rates.json`.
3. **Load** inserts the normalized rows into `data/rates.db`, creating the `exchange_rates` table when necessary.

The SQLite table has the following schema:

```sql
CREATE TABLE exchange_rates (
	date TEXT,
	currency TEXT,
	rate REAL,
	PRIMARY KEY (date, currency)
);
```

Repeated runs for the same date are safe because the load task uses `INSERT OR IGNORE`.

## Technology

- Apache Airflow 2.10.5
- Docker Compose
- CeleryExecutor with Redis as the broker
- PostgreSQL 13 for Airflow metadata
- Python and the `requests` library for extraction
- SQLite for the pipeline output
- ExchangeRate-API's public, unauthenticated latest-rates endpoint

## Prerequisites

- Docker Desktop with Docker Compose support
- At least 4 GB of memory allocated to Docker and at least 2 CPUs recommended
- Internet access from the Airflow containers so the extraction task can reach the API
- Port `8080` available for the Airflow webserver

## Start the project

From PowerShell, open the project directory and initialize Airflow's metadata database and default user:

```powershell
cd D:\Coding\airflow-exchange-rates
docker compose up airflow-init
```

Start the remaining services in the background:

```powershell
docker compose up -d
```

Open [http://localhost:8080](http://localhost:8080) and sign in with:

| Setting | Default |
| --- | --- |
| Username | `airflow` |
| Password | `airflow` |

The credentials can be changed before initialization with `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` environment variables. Do not use the defaults outside local development.

In the Airflow UI:

1. Find `exchange_rate_pipeline` in the DAG list.
2. Unpause it using the toggle in the DAG row.
3. Trigger a run manually with the play button.
4. Open the run and confirm that `extract`, `transform`, and `load` finish successfully.

The DAG is scheduled with `@daily`, but manual triggering is useful for testing immediately.

## Verify the output

The project directories are mounted into the containers, so the output is available on the host:

```text
data/
├── raw_rates.json       # Complete response from the API
├── clean_rates.json     # Four normalized currency rows
└── rates.db             # SQLite database created by the load task
```

To inspect the SQLite rows from PowerShell, if the `sqlite3` command is installed locally:

```powershell
sqlite3 data\rates.db "SELECT date, currency, rate FROM exchange_rates ORDER BY date, currency;"
```

Alternatively, open `data/rates.db` with any SQLite browser. Airflow task logs are written to `logs/`, and the DAG source is in `dags/exchange_rate_dag.py`.

## Services and ports

| Service | Purpose |
| --- | --- |
| `airflow-webserver` | Airflow UI on `localhost:8080` |
| `airflow-scheduler` | Checks the schedule and creates task instances |
| `airflow-worker` | Executes DAG tasks through Celery |
| `airflow-triggerer` | Runs deferred Airflow tasks |
| `postgres` | Stores Airflow metadata |
| `redis` | Celery task broker |
| `airflow-init` | Applies database migrations and creates the admin user |

Flower can be started for Celery monitoring:

```powershell
docker compose --profile flower up -d
```

Then open [http://localhost:5555](http://localhost:5555). Flower is disabled by default.

## Useful commands

View the status of all containers:

```powershell
docker compose ps
```

Follow Airflow service logs:

```powershell
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker
```

Stop the stack while keeping the PostgreSQL volume and generated files:

```powershell
docker compose down
```

Stop the stack and remove the PostgreSQL metadata volume, then initialize it again on the next start:

```powershell
docker compose down -v
docker compose up airflow-init
docker compose up -d
```

The `down -v` command removes Airflow metadata, but it does not remove files in the bind-mounted `data/` or `logs/` directories.

## Project structure

```text
.
├── dags/
│   └── exchange_rate_dag.py  # DAG definition and ETL functions
├── data/                     # Host-mounted pipeline outputs
├── logs/                     # Host-mounted Airflow task logs
├── plugins/                  # Local Airflow plugins directory
├── config/                   # Optional Airflow configuration files
├── dag_graph.png             # DAG graph screenshot
├── docker-compose.yaml       # Local Airflow cluster definition
└── README.md
```

## Troubleshooting

### The webserver is not available

Check that initialization completed and that the services are healthy:

```powershell
docker compose ps
docker compose logs airflow-init
docker compose logs airflow-webserver
```

If port `8080` is already in use, change the host side of the mapping in `docker-compose.yaml`, for example from `8080:8080` to `8081:8080`, and use `http://localhost:8081`.

### The DAG does not appear

Confirm that `dags/exchange_rate_dag.py` is present and check scheduler logs:

```powershell
docker compose logs -f airflow-scheduler
```

The Compose configuration starts new DAGs paused. Unpause `exchange_rate_pipeline` in the UI before waiting for its daily schedule.

### The extract task fails

The task requires outbound network access and a successful response from the public API. Inspect the task log in Airflow and test connectivity from the worker:

```powershell
docker compose exec airflow-worker python -c "import requests; print(requests.get('https://open.er-api.com/v6/latest/USD', timeout=30).status_code)"
```

### The load task does not create a database

Make sure the `data/` directory is writable and that the transform task completed first. The task order is fixed as `extract >> transform >> load`, so a failed upstream task prevents loading.

## DAG graph

![DAG graph](dag_graph.png)

## Notes

- This Compose configuration is intended for local development and learning, not production deployment.
- Exchange rates are fetched from an external service and can change independently of this repository.
- The API response and output data are generated artifacts. Avoid committing credentials or sensitive data to the repository.
