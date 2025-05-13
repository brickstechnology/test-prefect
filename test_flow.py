from prefect import flow, task

# import openai
# from google.cloud import bigquery

# openai.api_key = "YOUR_OPENAI_API_KEY"


@task
def fetch_data():
    return [
        "Customer said the delivery was late.",
        "Product quality is excellent.",
        "The customer service was helpful.",
        "The product was not as described.",
    ]


@task
def transform_text(text):
    return "hello"


@task
def load_to_bigquery(summaries):
    return "summaries"


@flow
def gpt_pipeline():
    raw_texts = fetch_data()
    summaries = [transform_text(text) for text in raw_texts]
    load_to_bigquery(summaries)


if __name__ == "__main__":
    gpt_pipeline()
