import random

from prefect import flow, task


@task
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(100), k=10)]


@task
def process_customer(customer_id: str) -> str:
    # Process a single customer
    return f"Processed {customer_id}"


@flow
def main(customer_ids: list[str]) -> list[str]:
    # Map the process_customer task across all customer IDs

    results = process_customer.map(customer_ids)
    return results


# if __name__ == "__main__":
#     main().serve(
#         name="test_flow2",
#         # parameters={"customer_ids": ["customer1", "customer2", "customer3"]},
#     )
