"""
Sub-Master Agent implementation for coordinating workers
"""
import time
from typing import Dict, Any, List
from base_agent import BaseAgent
from worker_agent import WorkerAgent
from storage import storage
from communication import message_bus

class SubMasterAgent(BaseAgent):
    def __init__(self, agent_id: str, max_workers: int = 3):
        super().__init__(agent_id, "SubMaster")
        self.max_workers = max_workers
        self.workers = []
        self.active_tasks = {}
        self.task_results = {}
        
        # Subscribe to task assignments and worker results
        message_bus.subscribe(f"tasks_{agent_id}", self.handle_task_assignment)
        message_bus.subscribe("worker_results", self.handle_worker_result)
        
        # Initialize workers
        self.initialize_workers()
        
    def initialize_workers(self):
        """Initialize worker agents"""
        for i in range(self.max_workers):
            worker_id = f"worker_{self.agent_id}_{i}"
            worker = WorkerAgent(worker_id)
            worker.start()
            self.workers.append(worker)
            
        print(f"🏭 {self.agent_id} initialized {len(self.workers)} workers")
        
    def handle_task_assignment(self, message: Dict[str, Any]):
        """Handle incoming task assignment from master"""
        if not self.is_running:
            return
            
        task_data = message.get("task_data", {})
        task_id = task_data.get("task_id")
        
        print(f"📥 {self.agent_id} received task: {task_id}")
        
        try:
            result = self.process_task(task_data)
            self.report_task_completion(task_id, result)
        except Exception as e:
            self.report_task_error(task_id, str(e))
            
    def process_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Coordinate workers to process document chunk"""
        task_id = task_data.get("task_id")
        document_chunk = task_data.get("document_chunk", [])
        chunk_id = task_data.get("chunk_id", 0)
        
        print(f"⚙️ {self.agent_id} processing {len(document_chunk)} documents")
        
        # Track this task
        self.active_tasks[task_id] = {
            "documents": document_chunk,
            "total_docs": len(document_chunk),
            "completed_docs": 0,
            "worker_results": [],
            "start_time": time.time()
        }
        
        # Assign documents to workers
        for i, document in enumerate(document_chunk):
            worker = self.workers[i % len(self.workers)]
            subtask_id = f"{task_id}_subtask_{chunk_id}_{i}"
            
            # Send assignment to worker
            message_bus.send_worker_assignment(worker.agent_id, {
                "task_id": task_id,
                "document": document,
                "subtask_id": subtask_id
            })
            
        # Wait for all workers to complete (simplified synchronization)
        self.wait_for_completion(task_id)
        
        return self.compile_results(task_id)
        
    def handle_worker_result(self, message: Dict[str, Any]):
        """Handle worker completion result"""
        worker_id = message.get("worker_id")
        result = message.get("result", {})
        subtask_id = result.get("subtask_id")
        
        if not subtask_id:
            return
            
        # Find the task this result belongs to
        task_id = subtask_id.split("_subtask_")[0]
        
        if task_id in self.active_tasks:
            self.active_tasks[task_id]["worker_results"].append(result)
            self.active_tasks[task_id]["completed_docs"] += 1
            
            print(f"📊 {self.agent_id} received result from {worker_id}: {subtask_id}")
            
    def wait_for_completion(self, task_id: str, timeout: int = 30):
        """Wait for all workers to complete their subtasks"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if task_id not in self.active_tasks:
                break
                
            task_info = self.active_tasks[task_id]
            if task_info["completed_docs"] >= task_info["total_docs"]:
                break
                
            time.sleep(0.5)
            
        print(f"⏱️ {self.agent_id} completed waiting for task {task_id}")
        
    def compile_results(self, task_id: str) -> Dict[str, Any]:
        """Compile results from all workers"""
        if task_id not in self.active_tasks:
            return {"error": "Task not found"}
            
        task_info = self.active_tasks[task_id]
        worker_results = task_info["worker_results"]
        
        # Aggregate results
        total_chunks = sum(r.get("chunks_processed", 0) for r in worker_results)
        total_words = sum(r.get("total_words", 0) for r in worker_results)
        successful_docs = len([r for r in worker_results if r.get("status") == "completed"])
        
        compiled_result = {
            "task_id": task_id,
            "sub_master_id": self.agent_id,
            "total_documents": task_info["total_docs"],
            "successful_documents": successful_docs,
            "total_chunks_processed": total_chunks,
            "total_words_processed": total_words,
            "processing_time": time.time() - task_info["start_time"],
            "worker_results": worker_results
        }
        
        # Store compiled result
        self.task_results[task_id] = compiled_result
        
        # Clean up active task
        del self.active_tasks[task_id]
        
        return compiled_result
        
    def report_task_completion(self, task_id: str, result: Dict[str, Any]):
        """Report task completion to master"""
        message_bus.broadcast_task_status(task_id, "submaster_completed", self.agent_id)
        
        # Send result to master
        message_bus.publish("submaster_results", {
            "sub_master_id": self.agent_id,
            "task_id": task_id,
            "result": result
        })
        
        print(f"✅ {self.agent_id} completed task: {task_id}")
        
    def report_task_error(self, task_id: str, error: str):
        """Report task error to master"""
        message_bus.broadcast_task_status(task_id, "submaster_error", self.agent_id)
        print(f"❌ {self.agent_id} error in task {task_id}: {error}")
        
    def stop(self):
        """Stop sub-master and all workers"""
        for worker in self.workers:
            worker.stop()
        super().stop()
