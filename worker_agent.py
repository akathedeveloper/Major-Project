""" Worker Agent implementation for document processing """
import time
import re
from typing import Dict, Any, List
from base_agent import BaseAgent
from storage import storage, vector_storage
from communication import message_bus


def call_llm_map(text: str, ctx_snippets: List[str]) -> Dict[str, Any]:
    """
    Stub for LLM map summarization. Replace with provider SDK.
    Returns a structured JSON with bullets and key terms.
    """
    bullets = []
    for s in text.split(". ")[:6]:
        if s.strip():
            bullets.append(s.strip()[:220])
    key_terms = list({w.lower() for w in re.findall(r'\b[A-Za-z]{6,}\b', text)})[:12]
    return {"summary_bullets": bullets, "key_terms": key_terms, "citations": []}


class WorkerAgent(BaseAgent):
    def __init__(self, agent_id: str):
        super().__init__(agent_id, "Worker")
        self.processed_documents: List[Dict[str, Any]] = []
        # Subscribe to worker assignments
        message_bus.subscribe(f"subtasks_{agent_id}", self.handle_subtask)

    def handle_subtask(self, message: Dict[str, Any]):
        """Handle incoming subtask assignment"""
        if not self.is_running:
            return
        subtask_data = message.get("subtask_data", {})
        print(f"📥 {self.agent_id} received subtask: {subtask_data.get('subtask_id')}")
        try:
            result = self.process_task(subtask_data)
            self.report_completion(subtask_data, result)
        except Exception as e:
            self.report_error(subtask_data, str(e))

    def process_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process individual document: chunk → vectorize → map summarize"""
        document = task_data.get("document", {})
        subtask_id = task_data.get("subtask_id", "unknown")
        print(f"⚙️ {self.agent_id} processing document: {document.get('id', 'unknown')}")
        time.sleep(0.2)  # small simulated latency

        try:
            content = document.get("content", "")
            doc_id = document.get("id", f"doc_{time.time()}")

            # Precomputed chunks from ingestion or fallback
            chunks_payload = document.get("chunks")
            if chunks_payload:
                chunks_texts = [c["text"] for c in chunks_payload]
            else:
                chunks_texts = self.chunk_document(content)

            # Vectorize raw chunks
            vector_storage.add_document_chunks(
                document_id=doc_id,
                chunks=chunks_texts,
                metadata={
                    "document_type": document.get("type", "unknown"),
                    "processed_by": self.agent_id
                }
            )

            # Map summarize each chunk with simple RAG
            processed_chunks = []
            total_words = 0
            for i, text in enumerate(chunks_texts):
                query = " ".join(text.split()[:40]) if text else ""
                sims = vector_storage.search_similar(query, n_results=2) if query else []
                ctx = [s["document"]["text"] for s in sims]
                map_out = call_llm_map(text, ctx)
                processed_chunks.append({
                    "chunk_id": f"{doc_id}_chunk_{i}",
                    "map_summary": map_out,
                    "word_count": len(text.split()),
                    "processed_at": time.time(),
                    "processed_by": self.agent_id
                })
                total_words += len(text.split())

            storage.store_document(doc_id, content, document.get("metadata", {}))

            result = {
                "subtask_id": subtask_id,
                "document_id": doc_id,
                "status": "completed",
                "chunks_processed": len(processed_chunks),
                "total_words": total_words,
                "processing_time": max(0.5, len(processed_chunks) * 0.1),
                "worker_id": self.agent_id,
                "map_results": processed_chunks
            }
            self.processed_documents.append(result)
            return result

        except Exception as e:
            # Create checkpoint for retry
            self.create_checkpoint({
                "subtask_id": subtask_id,
                "document_id": document.get("id"),
                "error": str(e),
                "retry_count": task_data.get("retry_count", 0) + 1
            })
            raise

    def chunk_document(self, content: str) -> List[str]:
        """Fallback chunking if ingestion didn't precompute chunks"""
        sentences = re.split(r'(?<=[.!?])\s+', content)
        chunks, buf = [], ""
        # approx 8000 chars ~ 2000 tokens
        target_chars = 8000
        for s in sentences:
            if len(buf) + len(s) < target_chars:
                buf += s + " "
            else:
                if buf:
                    chunks.append(buf.strip())
                buf = s + " "
        if buf:
            chunks.append(buf.strip())
        return chunks or [content]

    def report_completion(self, task_data: Dict[str, Any], result: Dict[str, Any]):
        """Report task completion"""
        task_id = task_data.get("task_id")
        if task_id:
            message_bus.broadcast_task_status(task_id, "worker_completed", self.agent_id)
        message_bus.publish("worker_results", {"worker_id": self.agent_id, "result": result})
        print(f"✅ {self.agent_id} completed subtask: {result['subtask_id']}")

    def report_error(self, task_data: Dict[str, Any], error: str):
        """Report task error"""
        task_id = task_data.get("task_id")
        if task_id:
            message_bus.broadcast_task_status(task_id, "worker_error", self.agent_id)
        print(f"❌ {self.agent_id} error in subtask: {error}")
