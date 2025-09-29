"""
Base agent class for AgenticOps MVP
"""
import time
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from storage import storage
from communication import message_bus

class BaseAgent(ABC):
    def __init__(self, agent_id: str, agent_type: str):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.status = "initializing"
        self.last_heartbeat = time.time()
        self.is_running = False
        self.heartbeat_thread = None
        
    def start(self):
        """Start the agent"""
        self.is_running = True
        self.status = "running"
        self.start_heartbeat()
        print(f"🤖 {self.agent_type} {self.agent_id} started")
        
    def stop(self):
        """Stop the agent"""
        self.is_running = False
        self.status = "stopped"
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=1)
        print(f"🛑 {self.agent_type} {self.agent_id} stopped")
        
    def start_heartbeat(self):
        """Start heartbeat in separate thread"""
        def heartbeat_loop():
            while self.is_running:
                self.send_heartbeat()
                time.sleep(5)  # Heartbeat every 5 seconds
                
        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        
    def send_heartbeat(self):
        """Send heartbeat signal"""
        self.last_heartbeat = time.time()
        storage.update_agent_heartbeat(self.agent_id, self.status)
        message_bus.broadcast_agent_heartbeat(self.agent_id, self.agent_type, self.status)
        
    def create_checkpoint(self, data: Dict[str, Any]):
        """Create checkpoint for fault tolerance"""
        checkpoint_id = storage.create_checkpoint(self.agent_id, data)
        print(f"💾 Checkpoint created: {checkpoint_id}")
        return checkpoint_id
        
    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Load latest checkpoint"""
        return storage.load_checkpoint(self.agent_id)
        
    @abstractmethod
    def process_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process assigned task - must be implemented by subclasses"""
        pass
