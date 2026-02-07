import time
import logging
from pathlib import Path
from celery import chord
import os

from split_pdf import split_pdf_to_chunks
from celery_app import convert_document, merge_chunks
from log_config import setup_logging

setup_logging()
logger = logging.getLogger("pipeline.main")

PAGES_PER_CHUNK = int(os.getenv("PAGES_PER_CHUNK", "10"))
QUEUES = os.getenv("CELERY_QUEUE_NAMES").split(",")

def run(pdf_path: str):
    start = time.monotonic()

    chunks = split_pdf_to_chunks(
        pdf_path,
        Path("temp_chunks"),
        PAGES_PER_CHUNK
    )

    header = [
        convert_document.s(chunk, i).set(queue=QUEUES[i % len(QUEUES)])
        for i, chunk in enumerate(chunks)
    ]

    out = Path("output_md") / f"{Path(pdf_path).stem}.md"
    result = chord(header)(merge_chunks.s(str(out)))
    result.get()

    total = time.monotonic() - start
    logger.info(f"PDF_DONE | time={total:.2f}s")

if __name__ == "__main__":
    run("sample/Sample5-LC.pdf")
