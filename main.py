""" AgenticOps MVP - Main Application Standalone Multi-Agent Document Processing System """
import time
from typing import List, Dict, Any
from master_agent import MasterAgent
from storage import storage
from config import config
from ingestion import ingest_file


class AgenticOpsDemo:
    def __init__(self):
        self.master_agent: MasterAgent = None

    def initialize(self):
        """Initialize the system"""
        print("🚀 Initializing AgenticOps MVP...")
        config.ensure_directories()
        self.master_agent = MasterAgent("master_001")
        self.master_agent.start()
        print("✅ AgenticOps MVP initialized successfully!")

    def create_sample_documents(self) -> List[Dict[str, Any]]:
        """Create sample documents for testing"""
        sample_docs = [
            {
                "id": "doc_001",
                "content": "Artificial Intelligence has revolutionized multiple industries. Machine learning algorithms can process vast amounts of data to identify patterns. Deep learning networks have shown remarkable success in image recognition and natural language processing.",
                "type": "article",
                "metadata": {"author": "AI Researcher", "category": "technology"}
            },
            {
                "id": "doc_002",
                "content": "Climate change represents one of the most significant challenges of our time. Rising global temperatures are causing sea levels to rise and weather patterns to shift. Renewable energy sources like solar and wind power offer sustainable alternatives.",
                "type": "report",
                "metadata": {"author": "Environmental Scientist", "category": "environment"}
            },
            {
                "id": "doc_003",
                "content": "The digital transformation of businesses has accelerated rapidly. Cloud computing provides scalable infrastructure for modern applications. Data analytics helps companies make informed decisions. Cybersecurity has become crucial as digital threats evolve.",
                "type": "analysis",
                "metadata": {"author": "Business Analyst", "category": "business"}
            }
        ]
        return sample_docs

    def run_demo(self):
        """Run the complete demo with in-memory small docs"""
        print("\n" + "="*80)
        print("🎯 STARTING AGENTICOPS MVP DEMONSTRATION")
        print("="*80)

        documents = self.create_sample_documents()
        print(f"\n📄 Created {len(documents)} sample documents")

        print("\n🔄 Starting document processing...")
        task_id = self.master_agent.process_document_batch(documents)

        print("\n⏳ Waiting for task completion...")
        time.sleep(5)

        result = self.master_agent.get_task_status(task_id)
        self.display_results(result)

        print("\n" + "="*80)
        print("✅ AGENTICOPS MVP DEMONSTRATION COMPLETED")
        print("="*80)

    def run_file(self, file_path: str):
        """Ingest a large file (PDF/TXT/EPUB) and run pipeline"""
        payload = ingest_file(file_path, metadata={"uploaded": True})
        documents = [{
            "id": payload["id"],
            "content": payload["content"],
            "chunks": payload["chunks"],
            "type": "textbook",
            "metadata": payload["metadata"]
        }]
        print(f"\n📚 Ingested file -> {payload['id']} with {len(payload['chunks'])} chunks")
        print("\n🔄 Starting document processing...")
        task_id = self.master_agent.process_document_batch(documents)
        return task_id

    def display_results(self, result: Dict[str, Any]):
        """Display processing results"""
        print("\n" + "="*60)
        print("📊 PROCESSING RESULTS")
        print("="*60)
        if result.get("status") == "completed":
            final_result = result["result"]
            print(f"📄 Total Documents: {final_result.get('total_documents_processed', 0)}")
            print(f"✅ Successful: {final_result.get('successful_documents', 0)}")
            print(f"❌ Failed: {final_result.get('failed_documents', 0)}")
            print(f"📝 Total Chunks: {final_result.get('total_chunks_processed', 0)}")
            print(f"📊 Success Rate: {final_result.get('success_rate', 0):.2%}")
            print(f"⏱️ Processing Time: {final_result.get('total_processing_time', 0):.2f}s")

            validation = final_result.get("validation", {})
            print(f"\n🔍 Validation Score: {validation.get('quality_score', 0):.2%}")
            print(f"🚨 Anomalies Detected: {len(validation.get('anomalies', []))}")

            synthesized = final_result.get("synthesized", {})
            for doc_id, syn in synthesized.items():
                print(f"\n📘 Final Summary for {doc_id} (first 8 bullets):")
                for b in syn.get("final_summary", [])[:8]:
                    print(f"  • {b}")
        else:
            print(f"Status: {result.get('status')}, data: {result.get('task_data')}")

    def shutdown(self):
        """Shutdown the system"""
        print("\n🛑 Shutting down AgenticOps MVP...")
        if self.master_agent:
            self.master_agent.stop()
        print("✅ Shutdown complete")


def main():
    """Main entry point"""
    demo = AgenticOpsDemo()
    try:
        demo.initialize()
        # Option A: run with small samples
        # demo.run_demo()

        # Option B: run with a large uploaded file
        # Ensure a file exists at ./data/book.pdf or change the path
        file_path = "./data/book.pdf"
        import os
        if os.path.exists(file_path):
            task_id = demo.run_file(file_path)
            # Wait for processing
            time.sleep(8)
            result = demo.master_agent.get_task_status(task_id)
            demo.display_results(result)
        else:
            print("ℹ️ No ./data/book.pdf found, running small demo instead.")
            demo.run_demo()
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
    finally:
        demo.shutdown()


if __name__ == "__main__":
    main()
