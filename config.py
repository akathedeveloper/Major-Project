""" Configuration for AgenticOps MVP - Standalone Version """
import os


class Config:
    # Agent Configuration
    MAX_WORKERS_PER_SUBMASTER = 4
    MAX_SUBMASTERS = 4
    HEARTBEAT_INTERVAL = 5           # seconds
    TASK_TIMEOUT = 600               # seconds

    # Document Processing
    SUPPORTED_FORMATS = ["txt", "md", "json", "pdf", "epub"]
    CHUNK_TOKENS = 2000
    CHUNK_OVERLAP_TOKENS = 200
    TOKENIZER = "gpt2"               # approximate budget heuristic

    # LLM (stubbed; wire actual provider if needed)
    LLM_PROVIDER = "openai"          # or "anthropic", "vllm"
    LLM_MODEL_MAP = "gpt-4o-mini"
    LLM_MODEL_REDUCE = "gpt-4o"
    LLM_MAX_RETRIES = 3
    LLM_TIMEOUT = 60

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
