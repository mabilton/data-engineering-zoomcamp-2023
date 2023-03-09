

## Set-Up


```bash
pip install -r requirements.txt
```

```bash
python3 download.py && \
python3 ingest.py
```

```bash

```

```bash
# Run dbt project:
export DUCKDB_PATH="$(pwd)/taxi.duckdb"
cd dbt/ny_taxi && \
dbt deps && \
dbt build && \
# Initialise piperider and create report:
piperider init && \
piperider diagnose && \
piperider run && \
cd ../..
```

