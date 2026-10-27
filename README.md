ReelRatings | Modern Data Engineering Pipeline

Overview

ReelRatings is a data engineering project built to demonstrate a modern data stack for transforming movie and ratings data into analytics-ready datasets.

The project focuses on data orchestration, data transformation, and warehouse modeling using Apache Airflow, Docker, dbt, and Snowflake.

Tech Stack

- Python — Pipeline logic and workflow tasks
- Apache Airflow — Workflow orchestration and scheduling
- Docker & Docker Compose — Containerized environment
- Snowflake — Cloud data warehouse
- dbt — SQL-based data transformation and data modeling
- Git & GitHub — Version control and project documentation

Data Pipeline

1. Ingest movie and ratings data into the data warehouse.
2. Use dbt staging models to prepare and standardize source data.
3. Build intermediate models to combine movie and rating information.
4. Create a mart model for analytics-ready movie ratings data.
5. Orchestrate pipeline workflows using Apache Airflow.

dbt Data Models

Staging

- "stg_movies.sql"
- "stg_ratings.sql"

Intermediate

- "int_movie_ratings.sql"

Marts

- "mart_movie_ratings.sql"

Project Structure

.
├── dags/
│   └── Airflow DAGs
├── dbt/
│   └── my_dbt_project/
│       └── models/
│           ├── staging/
│           ├── intermediate/
│           └── marts/
├── Dockerfile
├── docker-compose.yaml
└── README.md

Key Learning Outcomes

- Building and organizing data pipelines
- Orchestrating workflows with Apache Airflow
- Containerizing services with Docker
- Transforming data with dbt
- Designing analytical data models for a cloud data warehouse

Author

Selwan Osama

Computer Science Student | Data Engineering Trainee
