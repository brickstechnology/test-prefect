from prefect import flow
from prefect.deployments.flow_runs import run_deployment


def get_customer_ids(n: int):
    return list(range(n))


@flow
def test_batch(input_list: list[int]):
    batch_size = 10
    for i in range(0, len(input_list), batch_size):
        batch = input_list[i : i + batch_size]
        run_deployment(
            name="main/test-map",
            parameters={"input_list": batch},
        )


if __name__ == "__main__":
    test_batch(get_customer_ids(100))
