# CLAUDE.md

This file guides Claude Code when working in this repository.

## What this is

An Apache Airflow 3 project (based on the DeepLearning.AI "Orchestrating Workflows for GenAI Applications" course). It runs a small RAG pipeline over **Information Systems (IS) theories**: theory descriptions are embedded with `fastembed` and stored in a local Weaviate vector database, then queried by semantic similarity. It runs locally via the Astro CLI.

## Commands

All commands use the Astro CLI from the project root:

- `astro dev start` — build the image and start all containers (Airflow scheduler, API server, triggerer, dag processor, Postgres metadata DB, plus the Weaviate instance from `docker-compose.override.yml`). Opens the Airflow UI at `http://localhost:8080/` (no credentials needed).
- `astro dev stop` — stop containers.
- `astro dev restart` — restart (needed to pick up changes to `requirements.txt`, `Dockerfile`, `packages.txt`, or `docker-compose.override.yml`; DAG file changes are picked up automatically).
- `astro dev pytest` — run the test suite in `tests/` inside a container.
- `astro dev pytest tests/dags/test_dag_example.py` — run a single test file.
- `astro deploy` — deploy to Astronomer (after `astro login`).

Requires Astro CLI >= 1.34.1.

## Setup

Create a `.env` file (copy from `.env_example`). It defines `AIRFLOW_CONN_MY_WEAVIATE_CONN`, the Airflow connection to the local Weaviate instance. An OpenAI API key can be added there but is **not required** for the pipelines to run.

## Architecture

- `dags/genai_dags/` — the RAG pipeline DAGs:
  - `fetch_data.py` — reads `.txt` files from `include/data/` (one theory per file), parses each Markdown file into `name`/`description`/`authors`/`seminal_articles` (dynamic task mapping, one mapped instance per file), creates `fastembed` vector embeddings (embedding `name` + `description`), and loads them into the Weaviate `Theories` collection (creating the collection if absent). On success it updates the `my_theory_vector_data` Asset. Schedule: `@hourly`.
  - `query_data.py` — triggered by the `my_theory_vector_data` Asset (data-aware scheduling). Embeds a query string (from the `query_str` param) and returns the nearest theories via `near_vector` search.
- `dags/practice_dags/` — standalone teaching examples (basic DAGs, dynamic task mapping). Not part of the RAG pipeline.
- `include/data/` — source IS theory files, one theory per `.txt` file in Markdown: a top-level `# <name>` heading, then `##` sections `Concise description of theory`, `Originating author(s)`, and `Seminal articles`. A section value of `N/A` is treated as empty.
- `tests/dags/test_dag_example.py` — DAG-integrity tests: no import errors, all DAGs tagged, retries >= 2.
- `docker-compose.override.yml` — defines the local Weaviate service (port 8081, gRPC 50051).

## Conventions

- DAGs use the Airflow 3 TaskFlow API: `from airflow.sdk import dag, task, chain, Asset`, with `@dag` and `@task` decorators.
- **Imports for provider/heavy packages (`fastembed`, `WeaviateHook`, `weaviate`) go inside the task function**, not at module top level, to keep DAG parsing fast and avoid import errors during DAG discovery.
- The two GenAI DAGs share constants `COLLECTION_NAME = "Theories"` and `EMBEDDING_MODEL_NAME = "BAAI/bge-small-en-v1.5"` — keep them in sync across both files.
- The Weaviate connection is referenced by id `"my_weaviate_conn"` via `WeaviateHook`.
- Cross-DAG coordination is done with the `Asset("my_theory_vector_data")` — `fetch_data` produces it (`outlets=`), `query_data` consumes it (`schedule=[Asset(...)]`).
- Add Python deps to `requirements.txt`, OS packages to `packages.txt`, then restart.
