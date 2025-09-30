""" In-memory storage simulation for AgenticOps MVP """
import time
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional


class InMemoryStorage:
    def __init__(self):
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.checkpoints: Dict[str, Dict[str, Any]] = {}
        self.agent_status: Dict[str, Dict[str, Any]] = {}
        self.results: Dict[str, Dict[str, Any]] = {}
        self.documents: Dict[str, Dict[str, Any]] = {}

    def store_task(self, task_data: Dict[str, Any]) -> str:
        """Store task information; preserve provided task_id"""
        task_id = task_data.get("task_id") or str(uuid.uuid4())
        self.tasks[task_id] = {
            **task_data,
            "task_id": task_id,
            "created_at": self._now(),
            "status": task_data.get("status", "pending")
        }
        return task_id

    def update_task_status(self, task_id: str, status: str, result_data: Optional[Dict] = None):
        """Update task status"""
        if task_id in self.tasks:
            self.tasks[task_id]["status"] = status
            self.tasks[task_id]["updated_at"] = self._now()
            if result_data is not None:
                self.tasks[task_id]["result"] = result_data

    def create_checkpoint(self, agent_id: str, checkpoint_data: Dict[str, Any]) -> str:
        """Create checkpoint for fault tolerance"""
        checkpoint_id = f"{agent_id}_{int(time.time())}"
        self.checkpoints[checkpoint_id] = {
            "agent_id": agent_id,
            "checkpoint_data": checkpoint_data,
            "created_at": self._now()
        }
        return checkpoint_id

    def load_checkpoint(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Load latest checkpoint for agent"""
        agent_checkpoints = {k: v for k, v in self.checkpoints.items() if v["agent_id"] == agent_id}
        if agent_checkpoints:
            latest = max(agent_checkpoints.items(), key=lambda x: x[1]["created_at"])
            return latest[1]["checkpoint_data"]
        return None

    def update_agent_heartbeat(self, agent_id: str, status: str):
        """Update agent heartbeat for monitoring"""
        self.agent_status[agent_id] = {"status": status, "last_heartbeat": self._now()}

    def store_document(self, doc_id: str, content: str, metadata: Dict[str, Any]):
        """Store document"""
        self.documents[doc_id] = {"content": content, "metadata": metadata, "stored_at": self._now()}

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task by ID"""
        return self.tasks.get(task_id)

    def get_all_tasks(self) -> Dict[str, Dict[str, Any]]:
        """Get all tasks"""
        return self.tasks.copy()

    @staticmethod
    def _now() -> str:
        return datetime.now().isoformat()


# Global storage instance
storage = InMemoryStorage()


class VectorStorage:
    """Simple vector storage simulation using word-overlap similarity"""
    def __init__(self):
        self.embeddings: Dict[str, Dict[str, int]] = {}
        self.documents: Dict[str, Dict[str, Any]] = {}

    def add_document_chunks(self, document_id: str, chunks: List[str], metadata: Dict[str, Any] = None):
        """Add document chunks with simple word-based embeddings"""
        for i, chunk in enumerate(chunks):
            chunk_id = f"{document_id}_chunk_{i}"
            words = chunk.lower().split()
            word_freq: Dict[str, int] = {}
            for w in words:
                word_freq[w] = word_freq.get(w, 0) + 1
            self.embeddings[chunk_id] = word_freq
            self.documents[chunk_id] = {
                "text": chunk,
                "metadata": metadata or {},
                "document_id": document_id,
                "chunk_index": i
            }

    def search_similar(self, query: str, n_results: int = 3) -> List[Dict[str, Any]]:
        """Simple similarity search"""
        query_words = set(query.lower().split())
        similarities = []
        for chunk_id, word_freq in self.embeddings.items():
            chunk_words = set(word_freq.keys())
            overlap = len(query_words.intersection(chunk_words))
            total = len(query_words.union(chunk_words))
            sim = overlap / total if total > 0 else 0
            similarities.append({
                "chunk_id": chunk_id,
                "similarity": sim,
                "document": self.documents[chunk_id]
            })
        similarities.sort(key=lambda x: x["similarity"], reverse=True)
        return similarities[:n_results]


# Global vector storage
vector_storage = VectorStorage()
