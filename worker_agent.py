"""
Worker Agent implementation for document processing
"""
import time
import re
from typing import Dict, Any, List
from base_agent import BaseAgent
from storage import storage, vector_storage
from communication import message_bus

class WorkerAgent(BaseAgent):
    def __init__(self, agent_id: str):
        super().__init__(agent_id, "Worker")
        self.processed_documents = []
        
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
        """Process individual document"""
        document = task_data.get("document", {})
        subtask_id = task_data.get("subtask_id", "unknown")
        
        print(f"⚙️ {self.agent_id} processing document: {document.get('id', 'unknown')}")
        
        # Simulate processing time
        time.sleep(1)
        
        try:
            # Extract content
            content = document.get("content", "")
            doc_id = document.get("id", f"doc_{time.time()}")
            
            # Chunk document
            chunks = self.chunk_document(content)
            
            # Process each chunk
            processed_chunks = []
            for i, chunk in enumerate(chunks):
                processed_chunk = {
                    "chunk_id": f"{doc_id}_chunk_{i}",
                    "text": chunk,
                    "word_count": len(chunk.split()),
                    "processed_at": time.time(),
                    "processed_by": self.agent_id
                }
                processed_chunks.append(processed_chunk)
                
            # Store in vector database
            vector_storage.add_document_chunks(
                document_id=doc_id,
                chunks=chunks,
                metadata={
                    "document_type": document.get("type", "unknown"),
                    "processed_by": self.agent_id
                }
            )
            
            # Store document in storage
            storage.store_document(doc_id, content, document.get("metadata", {}))
            
            result = {
                "subtask_id": subtask_id,
                "document_id": doc_id,
                "status": "completed",
                "chunks_processed": len(processed_chunks),
                "total_words": sum(chunk["word_count"] for chunk in processed_chunks),
                "processing_time": 1.0,
                "worker_id": self.agent_id
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
        """Split document into chunks"""
        # Simple chunking by sentences
        sentences = re.split(r'[.!?]+', content)
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
                
            if len(current_chunk) + len(sentence) < 200:  # Chunk size from config
                current_chunk += sentence + ". "
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = sentence + ". "
                
        if current_chunk:
            chunks.append(current_chunk.strip())
            
        return chunks if chunks else [content]
        
    def report_completion(self, task_data: Dict[str, Any], result: Dict[str, Any]):
        """Report task completion"""
        task_id = task_data.get("task_id")
        if task_id:
            message_bus.broadcast_task_status(task_id, "worker_completed", self.agent_id)
            
        # Send result to sub-master
        message_bus.publish("worker_results", {
            "worker_id": self.agent_id,
            "result": result
        })
        
        print(f"✅ {self.agent_id} completed subtask: {result['subtask_id']}")
        
    def report_error(self, task_data: Dict[str, Any], error: str):
        """Report task error"""
        task_id = task_data.get("task_id")
        if task_id:
            message_bus.broadcast_task_status(task_id, "worker_error", self.agent_id)
            
        print(f"❌ {self.agent_id} error in subtask: {error}")
