import fitz
from pathlib import Path
import logging

logger = logging.getLogger("pipeline.split")

def split_pdf_to_chunks(pdf_path: str, out_dir: Path, pages_per_chunk: int):
    out_dir.mkdir(parents=True, exist_ok=True)

    doc = fitz.open(pdf_path)
    total = doc.page_count
    chunks = []

    for start in range(0, total, pages_per_chunk):
        end = min(start + pages_per_chunk - 1, total - 1)
        chunk = fitz.open()
        chunk.insert_pdf(doc, from_page=start, to_page=end)

        out = out_dir / f"chunk_{start+1}_{end+1}.pdf"
        chunk.save(out)
        chunk.close()
        chunks.append(str(out))

        logger.info(f"CHUNK_CREATED | {start+1}-{end+1}")

    doc.close()
    return chunks
