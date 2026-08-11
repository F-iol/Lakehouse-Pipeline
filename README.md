
# Project Overview

Initial Diagram for project architecture

```mermaid
flowchart TD
    A[Python Event Producer] --> B[Kafka Topic]
    B --> C[PySpark Structured Streaming]
    C --> D[Localhost]
    C --> E[AWS S3]
    E --> F[dbt Silver & Gold]
    G[Airflow] --> F
```