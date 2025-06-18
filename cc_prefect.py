import asyncio
import random
import time

from prefect import flow, task
from prefect.futures import wait


def get_customer_ids():
    return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


@task(tags=["send-email"])
async def send_email_a(customer_id: int):
    time.sleep(random.randint(0, 2))


@task(tags=["send-email"])
async def send_email_b(customer_id: int):
    time.sleep(random.randint(0, 2))


@task(tags=["write-to-db"])
async def write_to_db(customer_id: int):
    time.sleep(random.randint(0, 2))


@flow
async def main():
    customer_ids = get_customer_ids()
    taska = send_email_a.map(customer_ids)
    taskb = send_email_b.map(customer_ids)
    taskc = write_to_db.map(customer_ids)
    wait(taska)
    wait(taskb)
    wait(taskc)


if __name__ == "__main__":
    asyncio.run(main())
