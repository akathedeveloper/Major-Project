"""
In-memory storage simulation for AgenticOps MVP
"""
import json
import time
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
import os

class InMemoryStorage:
    def __init__(self):
        self.tasks = {}
        self.checkpoints = {}
        self.agent_status = {}
        self.results = {}
        self.documents = {}
        
    def store_task(self, task_data: Dict[str, Any]) -> str:
        """Store task information"""
        task_id = str(uuid.uuid4())
        task_data.update({
            "task_id": task_id,
            "created_at": datetime.now().isoformat(),
            "status": "pending"
        })
        self.tasks[task_id] = task_data
        return task_id
        
    def update_task_status(self, task_id: str, status: str, result_data: Optional[Dict] = None):
        """Update task status"""
        if task_id in self.tasks:
            self.tasks[task_id]["status"] = status
            self.tasks[task_id]["updated_at"] = datetime.now().isoformat()
            if result_data:
                self.tasks[task_id]["result"] = result_data
                
    def create_checkpoint(self, agent_id: str, checkpoint_data: Dict[str, Any]) -> str:
        """Create checkpoint for fault tolerance"""
        checkpoint_id = f"{agent_id}_{int(time.time())}"
        self.checkpoints[checkpoint_id] = {
            "agent_id": agent_id,
            "checkpoint_data": checkpoint_data,
            "created_at": datetime.now().isoformat()
        }
        return checkpoint_id
        
    def load_checkpoint(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Load latest checkpoint for agent"""
        agent_checkpoints = {k: v for k, v in self.checkpoints.items() 
                           if v["agent_id"] == agent_id}
        if agent_checkpoints:
            latest = max(agent_checkpoints.items(), key=lambda x: x[1]["created_at"])
            return latest[1]["checkpoint_data"]
        return None
        
    def update_agent_heartbeat(self, agent_id: str, status: str):
        """Update agent heartbeat for monitoring"""
        self.agent_status[agent_id] = {
            "status": status,
            "last_heartbeat": datetime.now().isoformat()
        }
        
    def store_document(self, doc_id: str, content: str, metadata: Dict[str, Any]):
        """Store document"""
        self.documents[doc_id] = {
            "content": content,
            "metadata": metadata,
            "stored_at": datetime.now().isoformat()
        }
        
    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task by ID"""
        return self.tasks.get(task_id)
        
    def get_all_tasks(self) -> Dict[str, Dict[str, Any]]:
        """Get all tasks"""
        return self.tasks.copy()

# Global storage instance
storage = InMemoryStorage()

class VectorStorage:
    """Simple vector storage simulation using cosine similarity"""
    def __init__(self):
        self.embeddings = {}
        self.documents = {}
        
    def add_document_chunks(self, document_id: str, chunks: List[str], metadata: Dict[str, Any] = None):
        """Add document chunks with simple word-based embeddings"""
        for i, chunk in enumerate(chunks):
            chunk_id = f"{document_id}_chunk_{i}"
            # Simple word frequency as embedding
            words = chunk.lower().split()
            word_freq = {}
            for word in words:
                word_freq[word] = word_freq.get(word, 0) + 1
            
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
            # Simple overlap similarity
            chunk_words = set(word_freq.keys())
            overlap = len(query_words.intersection(chunk_words))
            total_words = len(query_words.union(chunk_words))
            similarity = overlap / total_words if total_words > 0 else 0
            
            similarities.append({
                "chunk_id": chunk_id,
                "similarity": similarity,
                "document": self.documents[chunk_id]
            })
            
        # Sort by similarity and return top results
        similarities.sort(key=lambda x: x["similarity"], reverse=True)
        return similarities[:n_results]

# Global vector storage
vector_storage = VectorStorage()
