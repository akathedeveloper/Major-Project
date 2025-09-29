"""
In-memory communication system for agent coordination
"""
import asyncio
import json
import time
from typing import Dict, Any, Callable, List
from collections import defaultdict
import logging

class MessageBus:
    def __init__(self):
        self.channels = defaultdict(list)
        self.subscribers = defaultdict(list)
        self.message_history = []
        
    def publish(self, channel: str, message: Dict[str, Any]):
        """Publish message to channel"""
        message_with_timestamp = {
            **message,
            "timestamp": time.time(),
            "channel": channel
        }
        
        # Store message
        self.channels[channel].append(message_with_timestamp)
        self.message_history.append(message_with_timestamp)
        
        # Call subscribers
        for callback in self.subscribers[channel]:
            try:
                callback(message_with_timestamp)
            except Exception as e:
                print(f"Error in subscriber callback: {e}")
                
        print(f"📡 Published to {channel}: {message}")
        
    def subscribe(self, channel: str, callback: Callable[[Dict[str, Any]], None]):
        """Subscribe to channel with callback"""
        self.subscribers[channel].append(callback)
        print(f"📡 Subscribed to channel: {channel}")
        
    def get_messages(self, channel: str, since: float = 0) -> List[Dict[str, Any]]:
        """Get messages from channel since timestamp"""
        return [msg for msg in self.channels[channel] if msg["timestamp"] > since]
        
    def broadcast_task_status(self, task_id: str, status: str, agent_id: str):
        """Broadcast task status update"""
        message = {
            "task_id": task_id,
            "status": status,
            "agent_id": agent_id
        }
        self.publish("task_status", message)
        
    def broadcast_agent_heartbeat(self, agent_id: str, agent_type: str, status: str):
        """Broadcast agent heartbeat"""
        message = {
            "agent_id": agent_id,
            "agent_type": agent_type,
            "status": status
        }
        self.publish("agent_heartbeat", message)
        
    def send_task_assignment(self, sub_master_id: str, task_data: Dict[str, Any]):
        """Send task assignment to sub-master"""
        message = {
            "sub_master_id": sub_master_id,
            "task_data": task_data
        }
        self.publish(f"tasks_{sub_master_id}", message)
        
    def send_worker_assignment(self, worker_id: str, subtask_data: Dict[str, Any]):
        """Send subtask assignment to worker"""
        message = {
            "worker_id": worker_id,
            "subtask_data": subtask_data
        }
        self.publish(f"subtasks_{worker_id}", message)

# Global message bus
message_bus = MessageBus()
