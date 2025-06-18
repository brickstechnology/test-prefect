import time
from pathlib import Path
from typing import List

import fitz  # PyMuPDF
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_dask import DaskTaskRunner


@task
def get_customer_ids() -> List[str]:
    """Fetch customer IDs from a database or API"""
    print("Fetching customer IDs...")
    time.sleep(0.5)  # Simulate database query
    return [f"customer{n}" for n in range(5)]


@task
def aggregate_results(results: List[str]) -> dict:
    """Aggregate the processing results"""
    print("Aggregating results...")
    return {"total_processed": len(results), "results": results, "status": "completed"}


@flow(
    task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": 2, "threads_per_worker": 2})
)
def main_dask_local() -> dict:
    """Main flow using local Dask cluster"""
    print("=== Prefect Flow with Local Dask Task Runner ===")

    # Get customer IDs
    customer_ids = get_customer_ids()

    # Process customers in parallel using Dask
    processing_results = process_customer_heavy.map(customer_ids)

    # Aggregate results
    final_result = aggregate_results(processing_results)

    return final_result


@flow
def main_sequential() -> dict:
    """Main flow without Dask (sequential processing for comparison)"""
    print("=== Prefect Flow without Dask (Sequential) ===")

    # Get customer IDs
    customer_ids = get_customer_ids()

    # Process customers sequentially
    processing_results = process_customer_heavy.map(customer_ids)

    # Aggregate results
    final_result = aggregate_results(processing_results)

    return final_result


@flow(task_runner=ConcurrentTaskRunner())
def main_concurrent() -> dict:
    """Main flow without Dask (sequential processing for comparison)"""
    print("=== Prefect Flow without Dask (Sequential) ===")

    # Get customer IDs
    customer_ids = get_customer_ids()

    # Process customers sequentially
    processing_results = process_customer_heavy.map(customer_ids)

    # Aggregate results
    final_result = aggregate_results(processing_results)

    return final_result


# Advanced example with custom Dask configuration
@flow(
    task_runner=DaskTaskRunner(
        cluster_kwargs={
            "n_workers": 4,
            "threads_per_worker": 1,
            "memory_limit": "10GB",
            "dashboard_address": ":8787",
        }
    )
)
def main_dask_advanced() -> dict:
    """Advanced flow with custom Dask configuration"""
    print("=== Advanced Prefect Flow with Custom Dask Config ===")

    # Get customer IDs
    customer_ids = get_customer_ids()

    # Process customers with resource-intensive tasks
    processing_results = process_customer_heavy.map(customer_ids)

    # Aggregate results
    final_result = aggregate_results(processing_results)

    return final_result


@task
def process_customer_heavy(customer_id: str) -> str:
    """Convert PDF to PNG images for each customer using PyMuPDF"""
    print(f"Converting PDF to PNG for {customer_id}...")

    from pathlib import Path

    import fitz  # PyMuPDF

    # Create output directory if it doesn't exist
    output_dir = Path("output_pngs")
    output_dir.mkdir(exist_ok=True)

    # Assuming customer_id contains the PDF filename
    pdf_path = "pdfs/2556_รวมกฎหมาย_ปกครอง_สว.pdf"

    try:
        # Open PDF document
        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        # Process each page
        for page_num in range(total_pages):
            # Get page and render to pixmap with 2x zoom for better quality
            page = doc[page_num]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))

            # Save as PNG with customer ID and page number
            output_path = output_dir / f"{customer_id}_page_{page_num + 1}.png"
            pix.save(str(output_path))

            # Clean up pixmap to free memory
            pix = None

        # Close document
        doc.close()
        return (
            f"Successfully converted PDF to PNG for {customer_id} ({total_pages} pages)"
        )

    except Exception as e:
        # Ensure document is closed in case of error
        if "doc" in locals() and not doc.is_closed:
            doc.close()
        return f"Error processing {customer_id}: {str(e)}"


@task
def process_customer_heavy_fragment(customer_id: str) -> str:
    """Convert PDF to PNG images for each customer using PyMuPDF"""
    print(f"Converting PDF to PNG for {customer_id}...")

    from pathlib import Path

    import fitz  # PyMuPDF

    # Create output directory if it doesn't exist
    output_dir = Path("output_pngs")
    output_dir.mkdir(exist_ok=True)

    # Assuming customer_id contains the PDF filename
    pdf_path = "pdfs/2556_รวมกฎหมาย_ปกครอง_สว.pdf"

    try:
        # Open PDF document
        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        # Process pages in parallel using Prefect's map
        results = process_page.map(
            [doc] * total_pages,
            range(total_pages),
            [output_dir] * total_pages,
            [customer_id] * total_pages,
        )

        doc.close()
        return (
            f"Successfully converted PDF to PNG for {customer_id} ({total_pages} pages)"
        )

    except Exception as e:
        # Ensure document is closed in case of error
        if "doc" in locals() and not doc.is_closed:
            doc.close()
        return f"Error processing {customer_id}: {str(e)}"


def prep_page_task(customer_ids: list[str]) -> tuple[list, list, list, list]:
    """Prepare data for parallel page processing"""
    print(f"Preparing page tasks for {customer_ids}...")

    from pathlib import Path

    import fitz  # PyMuPDF

    # Create output directory if it doesn't exist
    output_dir = Path("output_pngs")
    output_dir.mkdir(exist_ok=True)

    # Assuming customer_id contains the PDF filename
    pdf_path = "pdfs/2556_รวมกฎหมาย_ปกครอง_สว.pdf"

    try:
        docs = []
        page_nums = []
        output_dirs = []
        customer_id_list = []

        for customer_id in customer_ids:
            # Open PDF document
            doc = fitz.open(pdf_path)
            total_pages = len(doc)

            # Prepare data for each page
            docs.extend([pdf_path] * total_pages)
            page_nums.extend(range(total_pages))
            output_dirs.extend([output_dir] * total_pages)
            customer_id_list.extend([customer_id] * total_pages)

        return docs, page_nums, output_dirs, customer_id_list

    except Exception as e:
        print(f"Error preparing page tasks: {str(e)}")
        # Return empty lists to maintain the expected tuple structure
        return [], [], [], []


@task
def process_page(
    pdf_path: str, page_num: int, output_dir: Path, customer_id: str
) -> str:
    doc = fitz.open(pdf_path)
    page = doc[page_num]
    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
    output_path = output_dir / f"{customer_id}_page_{page_num + 1}.png"
    pix.save(str(output_path))
    return f"Successfully converted page {page_num + 1} to PNG"


@flow(
    task_runner=DaskTaskRunner(
        cluster_kwargs={
            "n_workers": 4,
            "threads_per_worker": 1,
            "memory_limit": "10GB",
            "dashboard_address": ":8787",
        }
    )
)
def main_dask_advanced_fragment() -> dict:
    """Advanced flow with custom Dask configuration"""
    print("=== Advanced Prefect Flow with Custom Dask Config ===")

    # Get customer IDs
    customer_ids = get_customer_ids()

    # Process customers with resource-intensive tasks
    doc, page_num, output_dir, customer_id = prep_page_task(customer_ids)
    processing_results = process_page.map(doc, page_num, output_dir, customer_id)

    return processing_results


@flow
def main_sequential_fragment() -> dict:
    """Main flow without Dask (sequential processing for comparison)"""
    print("=== Prefect Flow without Dask (Sequential) ===")
    customer_ids = get_customer_ids()
    doc, page_num, output_dir, customer_id = prep_page_task(customer_ids)
    processing_results = process_page.map(doc, page_num, output_dir, customer_id)
    return processing_results


if __name__ == "__main__":
    import asyncio

    async def run_examples():
        print("Running Prefect + Dask examples...\n")

        # 1. Sequential processing for comparison
        print("1. Running sequential processing:")
        start_time = time.time()
        result2 = main_sequential()
        sequential_time = time.time() - start_time
        print(f"Result: {result2}")
        print(f"Time: {sequential_time:.2f} seconds\n")

        # 1.1 Concurrent processing
        print("1.1 Running concurrent processing:")
        start_time = time.time()
        result5 = main_concurrent()
        concurrent_time = time.time() - start_time
        print(f"Result: {result5}")
        print(f"Time: {concurrent_time:.2f} seconds\n")

        # 2. Advanced Dask configuration
        print("2. Running with advanced Dask configuration:")
        start_time = time.time()
        result3 = main_dask_advanced()
        advanced_time = time.time() - start_time
        print(f"Result: {result3}")
        print(f"Time: {advanced_time:.2f} seconds\n")

        # 3. Advanced Dask configuration with fragment processing
        print("3. Running with advanced Dask configuration with fragment processing:")
        start_time = time.time()
        result4 = main_dask_advanced_fragment()
        advanced_fragment_time = time.time() - start_time
        print(f"Result: {result4}")
        print(f"Time: {advanced_fragment_time:.2f} seconds\n")

        # 4. Sequential processing with fragment processing
        print("4. Running with sequential processing with fragment processing:")
        start_time = time.time()
        result5 = main_sequential_fragment()
        sequential_fragment_time = time.time() - start_time
        print(f"Result: {result5}")
        print(f"Time: {sequential_fragment_time:.2f} seconds\n")

        print("=== Performance Summary ===")
        print(f"Sequential: {sequential_time:.2f}s")
        print(f"Concurrent: {concurrent_time:.2f}s")
        print(f"Advanced Dask: {advanced_time:.2f}s")
        print(f"Advanced Dask with fragment processing: {advanced_fragment_time:.2f}s")
        print(f"Sequential with fragment processing: {sequential_fragment_time:.2f}s")
        # print(f"Multiprocessing: {mp_time:.2f}s")

        # Calculate speedups
        if sequential_time > advanced_time:
            print(f"Dask speedup: {sequential_time / advanced_time:.2f}x")
        if sequential_time > advanced_fragment_time:
            print(
                f"Dask with fragment processing speedup: {sequential_time / advanced_fragment_time:.2f}x"
            )

        # Compare which is fastest
        times = {
            "Sequential": sequential_time,
            "Concurrent": concurrent_time,
            "Advanced Dask": advanced_time,
            "Advanced Dask with fragment processing": advanced_fragment_time,
            "Sequential with fragment processing": sequential_fragment_time,
        }
        fastest = min(times, key=times.get)
        print(f"Fastest approach: {fastest} ({times[fastest]:.2f}s)")

    # Run the examples
    asyncio.run(run_examples())

    # To connect to an external Dask cluster, uncomment below:
    # print("\n4. To run with external Dask cluster:")
    # print("First start a Dask cluster:")
    # print("  dask-scheduler")
    # print("  dask-worker tcp://localhost:8786")
    # print("Then run:")
    # print("  result = main_dask_cluster()")
