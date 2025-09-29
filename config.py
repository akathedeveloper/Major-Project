"""
Configuration for AgenticOps MVP - Standalone Version
"""
import json
import os
from typing import Dict, Any, List

class Config:
    # Agent Configuration
    MAX_WORKERS_PER_SUBMASTER = 3
    MAX_SUBMASTERS = 2
    HEARTBEAT_INTERVAL = 5  # seconds
    TASK_TIMEOUT = 30  # seconds
    
    # Document Processing
    SUPPORTED_FORMATS = ["txt", "md", "json"]
    CHUNK_SIZE = 200  # characters
    
    # Storage paths
    DATA_DIR = "./data"
    CHECKPOINT_DIR = "./checkpoints"
    RESULTS_DIR = "./results"
    
    @classmethod
    def ensure_directories(cls):
        """Create necessary directories"""
        for dir_path in [cls.DATA_DIR, cls.CHECKPOINT_DIR, cls.RESULTS_DIR]:
            os.makedirs(dir_path, exist_ok=True)

config = Config()
