# ingestion.py
import os
import re
import uuid
from typing import Dict, List, Any, Tuple, Optional
from storage import storage
from config import config


def _normalize(text: str) -> str:
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _approx_token_len(s: str) -> int:
    # rough estimate: 4 chars ≈ 1 token
    return max(1, int(len(s) / 4))


def token_chunk(text: str, chunk_tokens: int, overlap_tokens: int) -> List[Tuple[str, Tuple[int, int]]]:
    words = text.split()
    chunks: List[Tuple[str, Tuple[int, int]]] = []
    buf: List[str] = []
    buf_tokens = 0
    i = 0
    while i < len(words):
        w = words[i]
        t = _approx_token_len(w)
        if buf_tokens + t <= chunk_tokens:
            buf.append(w)
            buf_tokens += t
            i += 1
        else:
            chunk_text = ' '.join(buf).strip()
            start_idx = i - len(buf)
            end_idx = i
            if chunk_text:
                chunks.append((chunk_text, (start_idx, end_idx)))
            # overlap
            overlap_words: List[str] = []
            ov_tokens = 0
            j = len(buf) - 1
            while j >= 0 and ov_tokens < overlap_tokens:
                wj = buf[j]
                ov_tokens += _approx_token_len(wj)
                overlap_words.insert(0, wj)
                j -= 1
            buf = overlap_words
            buf_tokens = sum(_approx_token_len(wj) for wj in buf)
    if buf:
        chunk_text = ' '.join(buf).strip()
        start_idx = max(0, len(words) - len(buf))
        chunks.append((chunk_text, (start_idx, len(words))))
    return chunks


def _read_sidecar_txt(path: str) -> Optional[str]:
    base, _ = os.path.splitext(path)
    sidecar = base + ".txt"
    if os.path.exists(sidecar):
        try:
            with open(sidecar, "r", encoding="utf-8", errors="ignore") as f:
                txt = _normalize(f.read())
                if txt.strip():
                    print(f"[ingestion] Using sidecar text: {sidecar}")
                    return txt
        except Exception as e:
            print(f"[ingestion] Failed reading sidecar: {e}")
    return None


def read_pdf_text(path: str) -> str:
    # Sidecar fast path
    sidecar = _read_sidecar_txt(path)
    if sidecar is not None:
        return sidecar

    # 1) pdfminer
    try:
        import pdfminer.high_level as pm
        text = pm.extract_text(path) or ""
        if text.strip():
            print("[ingestion] Extracted with pdfminer")
            return _normalize(text)
        else:
            print("[ingestion] pdfminer returned empty text")
    except Exception as e:
        print(f"[ingestion] pdfminer failed: {e}")

    # 2) PyPDF2
    try:
        import PyPDF2
        with open(path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            pages = []
            for idx, p in enumerate(reader.pages):
                try:
                    pages.append(p.extract_text() or "")
                except Exception as pe:
                    print(f"[ingestion] PyPDF2 page {idx} failed: {pe}")
                    pages.append("")
            text = "\n".join(pages)
            if text.strip():
                print("[ingestion] Extracted with PyPDF2")
                return _normalize(text)
            else:
                print("[ingestion] PyPDF2 returned empty text")
    except Exception as e:
        print(f"[ingestion] PyPDF2 failed: {e}")

    # 3) OCR fallback (scanned PDFs)
    try:
        from pdf2image import convert_from_path
        import pytesseract
        from PIL import Image  # noqa

        print("[ingestion] Falling back to OCR; this may take a while...")
        images = convert_from_path(path)  # requires poppler
        ocr_pages = []
        for i, img in enumerate(images):
            try:
                gray = img.convert("L")
                txt = pytesseract.image_to_string(gray, lang="eng")
                ocr_pages.append(txt or "")
            except Exception as oe:
                print(f"[ingestion] OCR failed on page {i}: {oe}")
                ocr_pages.append("")
        text = "\n".join(ocr_pages)
        if text.strip():
            print("[ingestion] Extracted with OCR")
            # write sidecar for faster subsequent runs
            base, _ = os.path.splitext(path)
            sidecar_path = base + ".txt"
            try:
                with open(sidecar_path, "w", encoding="utf-8") as f:
                    f.write(text)
                print(f"[ingestion] Wrote sidecar text: {sidecar_path}")
            except Exception as we:
                print(f"[ingestion] Failed writing sidecar: {we}")
            return _normalize(text)
    except Exception as e:
        print(f"[ingestion] OCR fallback failed: {e}")

    raise RuntimeError("Failed to extract text from PDF")


def ingest_file(path: str, doc_id: str = None, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    if ext not in config.SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format: {ext}")
    if not doc_id:
        doc_id = f"doc_{uuid.uuid4().hex[:8]}"

    if ext == "pdf":
        text = read_pdf_text(path)
    else:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            text = _normalize(f.read())

    storage.store_document(doc_id, text, metadata or {"source_path": path, "ext": ext})

    chunks = token_chunk(text, config.CHUNK_TOKENS, config.CHUNK_OVERLAP_TOKENS)
    return {
        "id": doc_id,
        "content": text,
        "chunks": [{"id": f"{doc_id}_chunk_{i}", "text": c[0], "span": c[1]} for i, c in enumerate(chunks)],
        "metadata": metadata or {}
    }
