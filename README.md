
# Project Overview

Initial Diagram for project architecture

```mermaid
flowchart TD
    A[Python Event Producer] --> B[Kafka Topic]
    B --> C[PySpark Structured Streaming]
    C --> D[AWS S3]
    D --> E[Snowflake Bronze Layer]
    E --> F[dbt Silver & Gold]
    G[Airflow] --> F
```