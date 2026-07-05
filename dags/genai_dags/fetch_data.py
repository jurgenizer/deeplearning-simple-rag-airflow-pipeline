from functools import lru_cache

from airflow.sdk import chain, dag, task, Asset
from pendulum import datetime, duration

COLLECTION_NAME = "Theories"
THEORY_FOLDER = "include/data"
EMBEDDING_MODEL_NAME = "BAAI/bge-small-en-v1.5"


@lru_cache(maxsize=1)
def _get_embedding_model():
    # Load (and download) the model once per worker process and reuse it across
    # mapped task instances instead of reloading it for every theory file.
    # The import stays inside the function to keep DAG parsing fast.
    from fastembed import TextEmbedding

    return TextEmbedding(EMBEDDING_MODEL_NAME)

def _my_callback_func(context):
    task_instance = context["task_instance"]
    dag_run = context["dag_run"]
    print(
        f"CALLBACK: Task {task_instance.task_id} "
        f"failed in DAG {dag_run.dag_id} at {dag_run.start_date}"
    )


def _parse_theory_file(text: str) -> dict:
    """Parse a scraped IS theory Markdown file into a dict.

    Expected format:
        # <Theory name>
        ## Concise description of theory
        <description or N/A>
        ## Originating author(s)
        <authors or N/A>
        ## Seminal articles
        <articles or N/A>
    """
    name = ""
    sections = {}
    current = None
    buf = []

    def flush():
        if current is not None:
            sections[current] = "\n".join(buf).strip()

    for line in text.splitlines():
        if line.startswith("# ") and not line.startswith("## "):
            name = line[2:].strip()
        elif line.startswith("## "):
            flush()
            current = line[3:].strip().lower()
            buf = []
        elif current is not None:
            buf.append(line)
    flush()

    def clean(value):
        value = (value or "").strip()
        return "" if value == "N/A" else value

    return {
        "name": name,
        "description": clean(sections.get("concise description of theory", "")),
        "authors": clean(sections.get("originating author(s)", "")),
        "seminal_articles": clean(sections.get("seminal articles", "")),
    }


@dag(
    start_date=datetime(2025, 6, 1),
    schedule="@hourly",
    tags=["genai", "rag"],
    default_args={
        "retries": 2,
        "retry_delay": duration(seconds=10),
        "on_failure_callback": _my_callback_func,
    },
    on_failure_callback=_my_callback_func,
)
def fetch_data():

    @task(retries=5, retry_delay=duration(seconds=2))
    def create_collection_if_not_exists() -> None:
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        hook = WeaviateHook("my_weaviate_conn")
        client = hook.get_conn()

        existing_collections = client.collections.list_all()
        existing_collection_names = existing_collections.keys()

        if COLLECTION_NAME not in existing_collection_names:
            print(f"Collection {COLLECTION_NAME} does not exist yet. Creating it...")
            collection = client.collections.create(name=COLLECTION_NAME)
            print(f"Collection {COLLECTION_NAME} created successfully.")
            print(f"Collection details: {collection}")

    _create_collection_if_not_exists = create_collection_if_not_exists()

    @task
    def list_theory_files() -> list:
        import os

        theory_files = [
            f for f in os.listdir(THEORY_FOLDER) if f.endswith(".txt")
        ]
        return theory_files

    _list_theory_files = list_theory_files()

    @task
    def transform_theory_file(theory_file: str) -> dict:
        import os

        with open(os.path.join(THEORY_FOLDER, theory_file), "r") as f:
            return _parse_theory_file(f.read())

    _transform_theory_file = transform_theory_file.expand(
        theory_file=_list_theory_files
    )

    @task
    def create_vector_embedding(theory_data: dict) -> list:
        embedding_model = _get_embedding_model()

        # Embed the theory name together with its description so that name-only
        # theories (with an N/A description) remain searchable by their name.
        text_to_embed = theory_data["name"]
        if theory_data["description"]:
            text_to_embed = f'{theory_data["name"]}. {theory_data["description"]}'

        embedding = list(map(float, next(embedding_model.embed([text_to_embed]))))
        return embedding

    _create_vector_embedding = create_vector_embedding.expand(
        theory_data=_transform_theory_file
    )

    @task(outlets=[Asset("my_theory_vector_data")])
    def load_embeddings_to_vector_db(
        list_of_theory_data: list, list_of_embeddings: list
    ) -> None:
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
        from weaviate.classes.data import DataObject
        from weaviate.util import generate_uuid5

        hook = WeaviateHook("my_weaviate_conn")
        client = hook.get_conn()
        collection = client.collections.get(COLLECTION_NAME)

        items = []
        for theory_data, emb in zip(list_of_theory_data, list_of_embeddings):
            item = DataObject(
                # Deterministic UUID derived from the theory name so that
                # re-running the (hourly) DAG upserts existing theories
                # instead of inserting duplicate copies each run.
                uuid=generate_uuid5(theory_data["name"]),
                properties={
                    "name": theory_data["name"],
                    "description": theory_data["description"],
                    "authors": theory_data["authors"],
                    "seminal_articles": theory_data["seminal_articles"],
                },
                vector=emb,
            )
            items.append(item)

        collection.data.insert_many(items)

    _load_embeddings_to_vector_db = load_embeddings_to_vector_db(
        list_of_theory_data=_transform_theory_file,
        list_of_embeddings=_create_vector_embedding,
    )

    chain(_create_collection_if_not_exists, _load_embeddings_to_vector_db)


fetch_data()
