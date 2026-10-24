# Enterprise Data Pipeline: Auto-Healing DLQ, Schema Drift & dbt

## Overview
This repository demonstrates a production-grade data engineering architecture designed to handle upstream data inconsistencies automatically. It features dynamic schema evolution, destructive drift mapping, PII masking, a Dead Letter Queue (DLQ) for quarantining invalid payloads, and idempotent, incrementally loaded Kimball dimensional models using dbt.

## Architecture & Data Flow
1. **Dynamic Ingestion & Security (Python)**: Processes incoming event batches.
   - *Security*: Cryptographically hashes PII (`email`) using a salted SHA-256 hash before it reaches the database.
   - *Destructive Drift*: Intercepts and corrects renamed columns (e.g., `event_name` -> `event_type`) via a contract mapping.
   - *Additive Drift*: Dynamically applies `ALTER TABLE` to PostgreSQL for new columns.
2. **DLQ Routing (SQLAlchemy)**: Hard database rejections (e.g., type collisions) are trapped gracefully, serialized to JSON, and routed to an `analytics.dlq_audit_log` table without halting the pipeline.
3. **Auto-Healing Sweep (Python)**: Parses the DLQ, applies business logic to repair malformed data (e.g., standardizing invalid ages to `-1`), and re-inserts the corrected micro-batch.
4. **Data Quality & Orchestration**: Uses `dbt build` as a strict circuit breaker. Soft logical anomalies are flagged via `dbt-expectations`, and relationship tests mathematically guarantee referential integrity before downstream marts are built.
5. **Idempotent Incremental Transformation (dbt)**: Staging models utilize MD5 file hashes and window functions to guarantee deduplication across retries, processing only late-arriving data via a lookback window.
6. **Dimensional Modeling (dbt)**: Downstream marts (e.g., `dim_users`) are dynamically built with auto-generated `-1` "Unknown" default rows utilizing reusable Jinja macros to handle late-arriving or missing dimensional references.

---

## 1. Database Infrastructure

**File: `infrastructure/01_init_dlq_audit_log.sql`**
```sql
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.raw_events (
    event_id VARCHAR(50),
    event_type VARCHAR(50),
    age INTEGER,
    filehash VARCHAR(64),
    batch_id VARCHAR(50),
    _loaded_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS analytics.dlq_audit_log (
    error_id SERIAL PRIMARY KEY,
    dlq_id VARCHAR(50),
    batch_id VARCHAR(50),
    table_name VARCHAR(100),
    error_category VARCHAR(50),
    error_message TEXT,
    raw_payload JSONB,
    status VARCHAR(20) DEFAULT 'QUARANTINED',
    failed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

---

## 2. Environment Variables

**File: `.env`**
```env
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
PII_SALT=enterprise_secret_salt_123
```

---

## 3. Ingestion & DLQ Routing Layer (Python)

**File: `plugins/ingest_to_postgres.py`**
```python
import os
import uuid
import hashlib
from datetime import datetime, timezone
import pandas as pd
import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError
from faker import Faker
from dotenv import load_dotenv

fake = Faker()
load_dotenv()

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
SCHEMA_NAME = "analytics"
TABLE_NAME = "raw_events"

PII_SALT = os.getenv("PII_SALT", "enterprise_secret_salt_123")
SCHEMA_CONTRACT_ALIASES = {
    "user_location": "location",
    "loc": "location",
    "client_age": "age",
    "event_name": "event_type",
    "user_email": "email"
}

DB_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URI)

def setup_schema():
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"))
        conn.commit()
    print(f"✅ Schema '{SCHEMA_NAME}' is ready.")

def mask_pii(email):
    if pd.isna(email) or email == "":
        return None
    salted = str(email).lower().strip() + PII_SALT
    return hashlib.sha256(salted.encode("utf-8")).hexdigest()

def hash_row(row):
    row_str = "".join(row.astype(str))
    return hashlib.md5(row_str.encode("utf-8")).hexdigest()

def add_metadata(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    return df.assign(
        filehash=lambda d: d.apply(hash_row, axis=1), batch_id=batch_id, _loaded_at=datetime.now(timezone.utc)
    )

def get_pg_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "VARCHAR"

def check_and_evolve_schema(df: pd.DataFrame, table_name: str, engine, schema: str = "analytics"):
    check_table_query = text(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        );
    """)
    with engine.connect() as conn:
        table_exists = conn.execute(check_table_query).scalar()
        if not table_exists:
            return
        col_query = text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table_name}';
        """)
        existing_cols = {row[0] for row in conn.execute(col_query)}

    df_cols = set(df.columns)
    new_cols = df_cols - existing_cols

    if new_cols:
        with engine.begin() as conn:
            for col in new_cols:
                pg_type = get_pg_type(df[col].dtype)
                alter_query = text(f"ALTER TABLE {schema}.{table_name} ADD COLUMN {col} {pg_type};")
                conn.execute(alter_query)
                print(f"[SCHEMA EVOLUTION] Added new column '{col}' of type {pg_type} to {table_name}.")

def ingest_batch(df: pd.DataFrame, table_name: str, engine, batch_id: str):
    rename_mapping = {}
    for col in df.columns:
        if col in SCHEMA_CONTRACT_ALIASES:
            rename_mapping[col] = SCHEMA_CONTRACT_ALIASES[col]
            print(f"[DRIFT DETECTED] Intercepted and renamed '{col}' back to '{SCHEMA_CONTRACT_ALIASES[col]}'")

    if rename_mapping:
        df = df.rename(columns=rename_mapping)

    if 'email' in df.columns:
        df['email_hash'] = df['email'].apply(mask_pii)
        df = df.drop(columns=['email'])
        print("[SECURITY] Hashed 'email' column to 'email_hash' and dropped raw PII.")

    check_and_evolve_schema(df, table_name, engine)
    df_prepared = df.copy().pipe(add_metadata, batch_id=batch_id)

    try:
        df_prepared.to_sql(table_name, engine, schema="analytics", if_exists="append", index=False)
        print(f"[SUCCESS] Batch {batch_id} successfully ingested into {table_name}.")
    except SQLAlchemyError as e:
        error_msg = str(e.__dict__.get("orig", e))
        print(f"[FAILED] Batch {batch_id} rejected by database. Routing to DLQ...")

        dlq_records = []
        for _, row in df.iterrows():
            dlq_records.append({
                "dlq_id": str(uuid.uuid4()),
                "batch_id": batch_id,
                "table_name": table_name,
                "error_category": "SCHEMA_TYPE_DRIFT",
                "error_message": error_msg,
                "raw_payload": json.dumps(row.to_dict()),
                "status": "PENDING",
                "failed_at": datetime.now(timezone.utc),
            })
        df_dlq = pd.DataFrame(dlq_records)
        df_dlq.to_sql("dlq_audit_log", engine, schema="analytics", if_exists="append", index=False)
        print(f"[DLQ ROUTED] {len(df_dlq)} records safely stored in analytics.dlq_audit_log.")

if __name__ == "__main__":
    setup_schema()

    good_data = pd.DataFrame({"event_id": ["EVT-101", "EVT-102"], "event_type": ["click", "view"], "age": [25, 30]})
    print("\n--- RUN 1: INITIAL BATCH ---")
    ingest_batch(good_data, "raw_events", engine, batch_id=f"BATCH-{uuid.uuid4().hex[:6]}")

    additive_data = pd.DataFrame({
        "event_id": ["EVT-103", "EVT-104"],
        "event_type": ["purchase", "click"],
        "age": [35, 40],
        "location": ["Manila", "Cebu"],
    })
    print("\n--- RUN 2: ADDITIVE DRIFT BATCH ---")
    ingest_batch(additive_data, "raw_events", engine, batch_id=f"BATCH-{uuid.uuid4().hex[:6]}")

    type_drift_data = pd.DataFrame({
        "event_id": ["EVT-105", "EVT-106"],
        "event_type": ["view", "scroll"],
        "age": ["unknown", "not_provided"],
        "location": ["Davao", "Manila"],
    })
    print("\n--- RUN 3: TYPE DRIFT BATCH ---")
    ingest_batch(type_drift_data, "raw_events", engine, batch_id=f"BATCH-{uuid.uuid4().hex[:6]}")

    destructive_pii_data = pd.DataFrame({
        "event_id": ["EVT-107", "EVT-108"],
        "event_name": ["login", "logout"],
        "client_age": [22, 45],
        "email": ["alvin@example.com", "test@test.com"]
    })
    print("\n--- RUN 4: DESTRUCTIVE DRIFT & PII BATCH ---")
    ingest_batch(destructive_pii_data, "raw_events", engine, batch_id=f"BATCH-{uuid.uuid4().hex[:6]}")
```

---

## 4. Auto-Healer DLQ Sweep (Python)

**File: `plugins/02_auto_healers.py`**
```python
import pandas as pd
import json
import uuid
from sqlalchemy import create_engine, text
from ingest_to_postgres import ingest_batch, DB_URI

engine = create_engine(DB_URI)

def heal_schema_type_drift(engine):
    with engine.connect() as conn:
        query = text("""
            SELECT error_id, raw_payload
            FROM analytics.dlq_audit_log
            WHERE status = 'PENDING'
            AND error_category = 'SCHEMA_TYPE_DRIFT'
        """)
        quarantined_records = conn.execute(query).fetchall()

    if not quarantined_records:
        print("[HEALER] No age-type drift records found in DLQ.")
        return

    healed_rows = []
    error_ids_to_update = []

    for record in quarantined_records:
        error_id = record[0]
        payload = record[1] if isinstance(record[1], dict) else json.loads(record[1])

        if not str(payload.get('age', '')).isnumeric():
            payload['age'] = -1

        healed_rows.append(payload)
        error_ids_to_update.append(error_id)

    df_healed = pd.DataFrame(healed_rows)

    if 'filehash' in df_healed.columns:
        df_healed = df_healed.drop(columns=['filehash', 'batch_id', '_loaded_at'])

    heal_batch_id = f"AUTO-HEAL-{uuid.uuid4().hex[:6]}"
    print(f"[HEALER] Attempting to re-ingest {len(healed_rows)} records as {heal_batch_id}...")

    ingest_batch(df_healed, "raw_events", engine, batch_id=heal_batch_id)

    with engine.connect() as conn:
        update_stmt = text(f"""
            UPDATE analytics.dlq_audit_log
            SET status = 'REPROCESSED'
            WHERE error_id IN ({','.join(map(str, error_ids_to_update))})
        """)
        conn.execute(update_stmt)
        conn.commit()
        print(f"[HEALER] Marked {len(error_ids_to_update)} records as REPROCESSED in DLQ.")

if __name__ == "__main__":
    heal_schema_type_drift(engine)
```

---

## 5. Airflow Orchestration

**File: `dags/03_dag_ingestion_pipeline.py`**
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from plugins.02_auto_healers import heal_schema_type_drift

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 24),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'idempotent_ingestion_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    run_ingestion = BashOperator(
        task_id='run_python_ingestion',
        bash_command='python /opt/airflow/plugins/ingest_to_postgres.py'
    )

    run_dlq_healer = PythonOperator(
        task_id='heal_quarantined_records',
        python_callable=heal_schema_type_drift,
        op_kwargs={'engine': '{{ var.value.db_engine_uri }}'}
    )

    run_dbt_build = BashOperator(
        task_id='dbt_build_pipeline',
        bash_command='cd /opt/airflow/transform && dbt build'
    )

    run_ingestion >> run_dlq_healer >> run_dbt_build
```

---

## 6. dbt Setup & Configurations

**File: `transform/dbt_project.yml`**
```yaml
name: 'idempotent_pipeline'
version: '1.0.0'
config-version: 2
profile: 'idempotent_pipeline'
model-paths: ["models"]
macro-paths: ["macros"]

models:
  idempotent_pipeline:
    staging:
      +materialized: view
```

**File: `transform/packages.yml`**
```yaml
packages:
  - package: calogica/dbt_expectations
    version: 0.10.1
```

**File: `transform/models/staging/schema.yml`**
```yaml
version: 2

sources:
  - name: analytics
    database: postgres
    tables:
      - name: raw_events
        columns:
          - name: event_id
            tests:
              - not_null
          - name: age
            tests:
              - dbt_expectations.expect_column_values_to_be_between:
                  min_value: -1
                  max_value: 120
                  config:
                    severity: warn
          - name: event_type
            tests:
              - dbt_expectations.expect_column_values_to_be_in_set:
                  value_set: ['click', 'purchase', 'login', 'logout', 'view', 'scroll']
                  config:
                    severity: warn

models:
  - name: stg_raw_events
    description: "The deduplicated, incrementally loaded staging events table."
    columns:
      - name: user_id
        description: "The user who performed the event. Coalesced to -1 if missing."
        tests:
          - not_null
          - relationships:
              to: ref('dim_users')
              field: user_id
```

---

## 7. dbt Macros

**File: `transform/macros/generate_default_row.sql`**
```sql
{% macro generate_default_dimension_row(column_mapping) %}
    {# Generates a default -1 row for Kimball dimensional models #}
    SELECT
    {% for column_name, default_value in column_mapping.items() %}
        {{ default_value }} AS {{ column_name }}
        {%- if not loop.last -%}, {%- endif -%}
    {% endfor %}
{% endmacro %}
```

---

## 8. dbt Models

**File: `transform/models/staging/stg_raw_events.sql`**
```sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

WITH raw_data AS (
    SELECT
        event_id,
        event_type,
        age,
        location,

        COALESCE(user_id, -1) AS user_id,
        COALESCE(email_hash, 'UNSPECIFIED_HASH') AS email_hash,

        filehash,
        batch_id,
        _loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY filehash
            ORDER BY _loaded_at DESC
        ) as dedupe_rn
    FROM {{ source('analytics', 'raw_events') }}

    {% if is_incremental() %}
    -- Incremental Processing: Only scan records loaded in the last 3 days
    WHERE _loaded_at >= (SELECT MAX(_loaded_at) - INTERVAL '3 days' FROM {{ this }})
    {% endif %}
)

SELECT
    event_id,
    event_type,
    age,
    location,
    user_id,
    email_hash,
    filehash,
    batch_id,
    _loaded_at
FROM raw_data
WHERE dedupe_rn = 1
```

**File: `transform/models/marts/dim_users.sql`**
```sql
{{ config(
    materialized='table',
    unique_key='user_id'
) }}

WITH event_users AS (
    -- Dynamically extract all unique user_ids from the events
    SELECT DISTINCT user_id
    FROM {{ ref('stg_raw_events') }}
    WHERE user_id != -1
),

source_users AS (
    -- Auto-generate profile data for the users
    SELECT
        user_id,
        'Auto-Generated User ' || CAST(user_id AS VARCHAR) AS first_name,
        'Active' AS account_status,
        CURRENT_TIMESTAMP AS registered_at
    FROM event_users
),

unknown_user AS (
    -- Using the reusable macro to generate the -1 row
    {{ generate_default_dimension_row({
        'user_id': -1,
        'first_name': "'Unknown User'",
        'account_status': "'Unspecified'",
        'registered_at': "'1900-01-01'::TIMESTAMP"
    }) }}
)

-- Combine the real users with the default unknown row
SELECT * FROM source_users
UNION ALL
SELECT * FROM unknown_user
```

### Visual Pipeline Flow
```mermaid
graph TD
    A[Upstream Systems] -->|Event JSON| B(Python Ingestion Script)
    B -->|Masks PII & Maps Alias| C{Schema Check}
    C -->|New Column| D[ALTER TABLE]
    C -->|Matches Schema| E[SQLAlchemy Insert]
    D --> E
    E -->|Success| F[(PostgreSQL: raw_events)]
    E -->|Type Collision / Fail| G[(PostgreSQL: dlq_audit_log)]
    G -->|02_auto_healers.py| B
    F -->|dbt build| H[stg_raw_events]
    H -->|dbt-expectations| I{Data Quality Tests}
    I -->|Pass| J[(dim_users / downstream marts)]
    I -->|Relationships Test Fail| K[Pipeline Halts]