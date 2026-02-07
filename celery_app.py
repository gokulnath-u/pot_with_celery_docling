import os
import time
import logging
import psutil
from celery import Celery
from celery.signals import worker_process_init
from pathlib import Path
import fitz
from dotenv import load_dotenv

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions

from log_config import setup_logging

# --------------------------------------------------
# Logging
# --------------------------------------------------
setup_logging()
load_dotenv()
logger = logging.getLogger("celery.ocr")

# --------------------------------------------------
# HARD CPU / THREAD LIMITS (ABSOLUTE)
# --------------------------------------------------
THREADS = os.getenv("OCR_THREADS", "1")

for v in [
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "TORCH_NUM_THREADS",
    "ORT_INTRA_OP_NUM_THREADS",
]:
    os.environ[v] = THREADS

os.environ["CUDA_VISIBLE_DEVICES"] = ""
os.environ["HF_HUB_OFFLINE"] = "1"

BROKER_URL = os.getenv("CELERY_BROKER_URL")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")

assert BROKER_URL, "CELERY_BROKER_URL not loaded"
assert RESULT_BACKEND, "CELERY_RESULT_BACKEND not loaded"

# --------------------------------------------------
# Celery App
# --------------------------------------------------
celery_app = Celery(
    "docling_ocr",
    broker=os.environ["CELERY_BROKER_URL"],
    backend=os.environ["CELERY_RESULT_BACKEND"],
)

app = celery_app

celery_app.conf.update(
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    broker_connection_retry_on_startup=True,
)

celery_app.conf.result_backend = os.environ["CELERY_RESULT_BACKEND"]
celery_app.conf.accept_content = ["json"]
celery_app.conf.task_serializer = "json"
celery_app.conf.result_serializer = "json"
celery_app.conf.worker_prefetch_multiplier = 1
celery_app.conf.task_acks_late = True
celery_app.conf.task_time_limit = 900        # hard kill
celery_app.conf.task_soft_time_limit = 850   # graceful warning


# --------------------------------------------------
# OCR Pipeline
# --------------------------------------------------
pipeline_options = PdfPipelineOptions(
    do_ocr=True,
    do_table_structure=False,
)

_converter = None

def get_converter():
    global _converter
    if _converter is None:
        pid = os.getpid()
        logger.info(f"MODEL_LOAD | pid={pid}")

        _converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options
                )
            }
        )

        logger.info(f"MODEL_READY | pid={pid}")
    return _converter

# --------------------------------------------------
# Worker Init (NUMA visibility)
# --------------------------------------------------
@worker_process_init.connect
def init_worker(**_):
    p = psutil.Process(os.getpid())
    logger.info(
        f"WORKER_INIT | pid={p.pid} | "
        f"rss={p.memory_info().rss/1e6:.1f}MB | "
        f"affinity={p.cpu_affinity()}"
    )
    get_converter()

# --------------------------------------------------
# OCR Task
# --------------------------------------------------
@celery_app.task(bind=True)
def convert_document(self, pdf_path: str, idx: int):
    pid = os.getpid()
    conv = get_converter()

    doc = fitz.open(pdf_path)
    pages = doc.page_count
    doc.close()

    t0 = time.monotonic()
    result = conv.convert(pdf_path)
    md = result.document.export_to_markdown()
    dt = time.monotonic() - t0

    out_dir = Path("temp_md")
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"chunk_{idx:04d}.md"
    out.write_text(md, encoding="utf-8")

    logger.info(
        f"CHUNK_OK | idx={idx} | pid={pid} | "
        f"pages={pages} | time={dt:.2f}s | pps={pages/dt:.2f}"
    )

    return {"idx": idx, "path": str(out)}

@celery_app.task
def merge_chunks(results, output_file: str):
    results = sorted(results, key=lambda r: r["idx"])

    with open(output_file, "w", encoding="utf-8") as f:
        for r in results:
            f.write(f"\n<!-- chunk {r['idx']} -->\n")
            f.write(Path(r["path"]).read_text(encoding="utf-8"))

    logger.info(f"MERGE_DONE | {output_file}")
    return output_file
