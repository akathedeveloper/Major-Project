""" Residual Agent implementation for result validation """
import time
from typing import Dict, Any, List
from base_agent import BaseAgent


class ResidualAgent(BaseAgent):
    def __init__(self, agent_id: str):
        super().__init__(agent_id, "Residual")
        self.validation_history: List[Dict[str, Any]] = []

    def validate_results(self, task_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate processing results and detect anomalies"""
        print(f"🔍 {self.agent_id} validating {len(task_results)} results")
        validation_report = {
            "validator_id": self.agent_id,
            "total_results": len(task_results),
            "successful": 0,
            "failed": 0,
            "anomalies": [],
            "quality_score": 0.0,
            "validation_time": time.time()
        }
        for result in task_results:
            ok = self.validate_single_result(result)
            if ok:
                validation_report["successful"] += 1
            else:
                validation_report["failed"] += 1
                validation_report["anomalies"].append({
                    "result_id": result.get("task_id", "unknown"),
                    "reason": self.get_failure_reason(result)
                })
        if validation_report["total_results"] > 0:
            validation_report["quality_score"] = validation_report["successful"] / validation_report["total_results"]
        self.validation_history.append(validation_report)
        print(f"📊 Validation complete: {validation_report['successful']}/{validation_report['total_results']} successful")
        return validation_report

    def validate_single_result(self, result: Dict[str, Any]) -> bool:
        """Validate a single processing result"""
        for field in ["task_id", "sub_master_id", "total_documents", "successful_documents", "worker_results"]:
            if field not in result:
                return False
        if result.get("successful_documents", 0) <= 0:
            return False
        worker_results = result.get("worker_results", [])
        if not worker_results:
            return False
        for wr in worker_results:
            if wr.get("status") != "completed":
                return False
            if wr.get("chunks_processed", 0) <= 0:
                return False
            if not wr.get("map_results"):
                return False
        return True

    def get_failure_reason(self, result: Dict[str, Any]) -> str:
        if "task_id" not in result:
            return "Missing task ID"
        if result.get("successful_documents", 0) == 0:
            return "No successful documents processed"
        if not result.get("worker_results"):
            return "No worker results"
        return "Unknown validation failure"

    def process_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process validation task"""
        results = task_data.get("results", [])
        validation_report = self.validate_results(results)
        return validation_report
