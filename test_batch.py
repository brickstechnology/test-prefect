import asyncio

from prefect import flow
from prefect.deployments.flow_runs import run_deployment


def get_customer_ids(n: int):
    return list(range(n))


@flow
async def test_batch(input_list: list[int]):
    batch_size = 10

    # Create all batches first
    batches = []
    for i in range(0, len(input_list), batch_size):
        batch = input_list[i : i + batch_size]
        batches.append(batch)

    # Run all deployments concurrently
    tasks = []
    for batch in batches:
        task = run_deployment(
            name="main/test-map",
            parameters={"input_list": batch},
            as_subflow=True,
        )
        tasks.append(task)

    # Wait for all deployments to complete concurrently
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(test_batch(get_customer_ids(100)))
