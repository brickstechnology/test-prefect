import random
import time

from prefect import flow, task
from prefect.futures import wait


def get_customer_ids():
    return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


@task(tags=["send-email"])
async def send_email_a(customer_id: int):
    time.sleep(random.randint(0, 2))
    if random.random() < 0.3:  # 30% chance of failure
        raise Exception(f"Random failure for customer {customer_id}")


@task(tags=["send-email"])
async def send_email_b(customer_id: int):
    time.sleep(random.randint(0, 2))


@task(tags=["write-to-db"])
async def write_to_db(customer_id: int):
    time.sleep(random.randint(0, 2))


@flow()
async def main(input_list: list[int]):
    taska = send_email_a.map(input_list)
    taskb = send_email_b.map(input_list)
    taskc = write_to_db.map(input_list)
    wait(taska)
    wait(taskb)
    wait(taskc)
