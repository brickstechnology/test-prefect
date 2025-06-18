import random
import time
from typing import List

import dask
from dask import delayed
from dask.distributed import Client


@delayed
def get_customer_ids() -> List[str]:
    """Fetch customer IDs from a database or API"""
    # Simulate some work
    time.sleep(0.1)
    return [f"customer{n}" for n in random.choices(range(100), k=10)]


@delayed
def process_customer(customer_id: str) -> str:
    """Process a single customer"""
    # Simulate some processing time
    time.sleep(0.2)
    return f"Processed {customer_id}"


def main_delayed():
    """Example using Dask delayed"""
    print("=== Dask Delayed Example ===")

    # Get customer IDs (lazy evaluation)
    customer_ids_task = get_customer_ids()

    # Process each customer in parallel (lazy evaluation)
    processing_tasks = [process_customer(cid) for cid in customer_ids_task]

    # Compute all tasks
    start_time = time.time()
    results = dask.compute(*processing_tasks)
    end_time = time.time()

    print(f"Processed {len(results)} customers in {end_time - start_time:.2f} seconds")
    for result in results:
        print(f"  {result}")

    return results


def main_futures():
    """Example using Dask distributed futures"""
    print("\n=== Dask Futures Example ===")

    # Start a local Dask client
    with Client(processes=False, threads_per_worker=2, n_workers=1) as client:
        print(f"Dask Dashboard: {client.dashboard_link}")

        # Submit tasks to the cluster
        customer_ids = [f"customer{n}" for n in random.choices(range(100), k=8)]

        # Submit processing tasks as futures
        futures = []
        for customer_id in customer_ids:
            future = client.submit(process_customer_sync, customer_id)
            futures.append(future)

        # Gather results
        start_time = time.time()
        results = client.gather(futures)
        end_time = time.time()

        print(
            f"Processed {len(results)} customers in {end_time - start_time:.2f} seconds"
        )
        for result in results:
            print(f"  {result}")

        return results


def process_customer_sync(customer_id: str) -> str:
    """Synchronous version for futures example"""
    time.sleep(0.2)
    return f"Processed {customer_id}"


def main_bag():
    """Example using Dask Bag for processing collections"""
    print("\n=== Dask Bag Example ===")

    import dask.bag as db

    # Create a bag of customer IDs
    customer_ids = [f"customer{n}" for n in random.choices(range(100), k=12)]
    bag = db.from_sequence(customer_ids, partition_size=3)

    # Process customers using bag operations
    start_time = time.time()
    results = bag.map(process_customer_sync).compute()
    end_time = time.time()

    print(f"Processed {len(results)} customers in {end_time - start_time:.2f} seconds")
    for result in results:
        print(f"  {result}")

    return results


if __name__ == "__main__":
    # Run different Dask examples
    main_delayed()
    main_futures()
    main_bag()

    print("\n=== Performance Comparison ===")
    print("Sequential processing:")
    customer_ids = [f"customer{n}" for n in range(5)]
    start_time = time.time()
    sequential_results = [process_customer_sync(cid) for cid in customer_ids]
    sequential_time = time.time() - start_time
    print(f"Sequential: {sequential_time:.2f} seconds")

    print("\nParallel processing with Dask:")
    start_time = time.time()
    parallel_tasks = [delayed(process_customer_sync)(cid) for cid in customer_ids]
    parallel_results = dask.compute(*parallel_tasks)
    parallel_time = time.time() - start_time
    print(f"Parallel: {parallel_time:.2f} seconds")
    print(f"Speedup: {sequential_time / parallel_time:.2f}x")
