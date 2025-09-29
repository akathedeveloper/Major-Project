"""
Master Agent implementation - orchestrates the entire workflow
"""
import time
from typing import Dict, Any, List
from base_agent import BaseAgent
from sub_master_agent import SubMasterAgent
from residual_agent import ResidualAgent
from storage import storage
from communication import message_bus

class MasterAgent(BaseAgent):
    def __init__(self, agent_id: str, max_submasters: int = 2):
        super().__init__(agent_id, "Master")
        self.max_submasters = max_submasters
        self.sub_masters = []
        self.residual_agent = None
        self.active_tasks = {}
        self.completed_tasks = {}
        
        # Subscribe to sub-master results
        message_bus.subscribe("submaster_results", self.handle_submaster_result)
        
        # Initialize sub-masters and residual agent
        self.initialize_sub_masters()
        self.initialize_residual_agent()
        
    def initialize_sub_masters(self):
        """Initialize sub-master agents"""
        for i in range(self.max_submasters):
            sub_master_id = f"submaster_{self.agent_id}_{i}"
            sub_master = SubMasterAgent(sub_master_id)
            sub_master.start()
            self.sub_masters.append(sub_master)
            
        print(f"🏢 {self.agent_id} initialized {len(self.sub_masters)} sub-masters")
        
    def initialize_residual_agent(self):
        """Initialize residual agent for validation"""
        self.residual_agent = ResidualAgent(f"residual_{self.agent_id}")
        self.residual_agent.start()
        print(f"🔍 {self.agent_id} initialized residual agent")
        
    def process_document_batch(self, documents: List[Dict[str, Any]]) -> str:
        """Main mapper function - splits work across sub-masters"""
        task_id = f"task_{int(time.time())}_{len(self.active_tasks)}"
        
        print(f"🎯 {self.agent_id} starting batch processing: {task_id}")
        print(f"📄 Processing {len(documents)} documents")
        
        # Store task in storage
        task_data = {
            "task_id": task_id,
            "documents": documents,
            "total_docs": len(documents),
            "sub_master_assignments": []
        }
        storage.store_task(task_data)
        
        # Track active task
        self.active_tasks[task_id] = {
            "total_docs": len(documents),
            "sub_master_results": [],
            "start_time": time.time(),
            "status": "processing"
        }
        
        # Split documents across sub-masters (Mapper phase)
        document_chunks = self.split_documents(documents)
        
        for i, chunk in enumerate(document_chunks):
            if i < len(self.sub_masters):
                sub_master = self.sub_masters[i]
                
                # Send task assignment
                message_bus.send_task_assignment(sub_master.agent_id, {
                    "task_id": task_id,
                    "document_chunk": chunk,
                    "chunk_id": i
                })
                
        # Update task data
        storage.update_task_status(task_id, "distributed")
        
        return task_id
        
    def split_documents(self, documents: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Split documents into chunks for sub-masters"""
        if not documents:
            return []
            
        chunk_size = max(1, len(documents) // len(self.sub_masters))
        chunks = []
        
        for i in range(0, len(documents), chunk_size):
            chunk = documents[i:i + chunk_size]
            chunks.append(chunk)
            
        # Ensure we don't have more chunks than sub-masters
        while len(chunks) > len(self.sub_masters):
            # Merge the last two chunks
            last_chunk = chunks.pop()
            chunks[-1].extend(last_chunk)
            
        return chunks
        
    def handle_submaster_result(self, message: Dict[str, Any]):
        """Handle completion result from sub-master"""
        sub_master_id = message.get("sub_master_id")
        task_id = message.get("task_id")
        result = message.get("result", {})
        
        if task_id not in self.active_tasks:
            return
            
        print(f"📊 {self.agent_id} received result from {sub_master_id}")
        
        # Store sub-master result
        self.active_tasks[task_id]["sub_master_results"].append(result)
        
        # Check if all sub-masters completed
        expected_results = len([sm for sm in self.sub_masters])
        actual_results = len(self.active_tasks[task_id]["sub_master_results"])
        
        if actual_results >= expected_results:
            self.finalize_task(task_id)
            
    def finalize_task(self, task_id: str):
        """Finalize task - Reducer phase"""
        if task_id not in self.active_tasks:
            return
            
        print(f"🔄 {self.agent_id} finalizing task: {task_id}")
        
        task_info = self.active_tasks[task_id]
        sub_master_results = task_info["sub_master_results"]
        
        # Aggregate results from all sub-masters (Reducer phase)
        final_result = self.aggregate_results(sub_master_results)
        final_result["task_id"] = task_id
        final_result["master_id"] = self.agent_id
        final_result["total_processing_time"] = time.time() - task_info["start_time"]
        
        # Validate results using residual agent
        validation_report = self.residual_agent.validate_results(sub_master_results)
        final_result["validation"] = validation_report
        
        # Store final result
        self.completed_tasks[task_id] = final_result
        storage.update_task_status(task_id, "completed", final_result)
        
        # Clean up active task
        del self.active_tasks[task_id]
        
        # Broadcast completion
        message_bus.broadcast_task_status(task_id, "master_completed", self.agent_id)
        
        print(f"✅ {self.agent_id} completed task: {task_id}")
        print(f"📊 Final result: {final_result['total_documents_processed']} docs, {final_result['total_chunks_processed']} chunks")
        
        return final_result
        
    def aggregate_results(self, sub_master_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate results from all sub-masters"""
        total_docs = sum(r.get("total_documents", 0) for r in sub_master_results)
        successful_docs = sum(r.get("successful_documents", 0) for r in sub_master_results)
        total_chunks = sum(r.get("total_chunks_processed", 0) for r in sub_master_results)
        total_words = sum(r.get("total_words_processed", 0) for r in sub_master_results)
        
        return {
            "total_documents_processed": total_docs,
            "successful_documents": successful_docs,
            "failed_documents": total_docs - successful_docs,
            "total_chunks_processed": total_chunks,
            "total_words_processed": total_words,
            "success_rate": successful_docs / total_docs if total_docs > 0 else 0,
            "sub_master_results": sub_master_results
        }
        
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Get current status of a task"""
        if task_id in self.completed_tasks:
            return {
                "status": "completed",
                "result": self.completed_tasks[task_id]
            }
        elif task_id in self.active_tasks:
            return {
                "status": "processing",
                "progress": self.active_tasks[task_id]
            }
        else:
            stored_task = storage.get_task(task_id)
            if stored_task:
                return {
                    "status": stored_task.get("status", "unknown"),
                    "task_data": stored_task
                }
            return {"status": "not_found"}
            
    def process_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process task - implementation of abstract method"""
        documents = task_data.get("documents", [])
        return self.process_document_batch(documents)
        
    def stop(self):
        """Stop master and all sub-agents"""
        for sub_master in self.sub_masters:
            sub_master.stop()
        if self.residual_agent:
            self.residual_agent.stop()
        super().stop()
