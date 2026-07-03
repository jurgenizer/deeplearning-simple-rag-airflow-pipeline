from airflow.sdk import dag, task, Asset

COLLECTION_NAME = "Theories"
EMBEDDING_MODEL_NAME = "BAAI/bge-small-en-v1.5"


@dag(
    schedule=[Asset("my_theory_vector_data")],
    tags=["genai", "rag"],
    default_args={"retries": 2},
    params={"query_str": "A theory explaining why people adopt new technology"},
)
def query_data():

    @task
    def search_vector_db_for_a_theory(**context):
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
        from fastembed import TextEmbedding

        query_str = context["params"]["query_str"]

        hook = WeaviateHook("my_weaviate_conn")
        client = hook.get_conn()

        embedding_model = TextEmbedding(EMBEDDING_MODEL_NAME)
        collection = client.collections.get(COLLECTION_NAME)

        query_emb = list(embedding_model.embed([query_str]))[0]

        results = collection.query.near_vector(
            near_vector=query_emb,
            limit=3,
        )
        for result in results.objects:
            props = result.properties
            print(f"Theory: {props['name']}")
            if props["authors"]:
                print(f"Originating author(s): {props['authors']}")
            if props["description"]:
                print(f"Description: {props['description']}")
            print("-" * 80)

    search_vector_db_for_a_theory()


query_data()
