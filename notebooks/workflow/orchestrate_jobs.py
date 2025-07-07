from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from datetime import datetime, timedelta
import time
from typing import Dict, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobOrchestrator:
    def __init__(self):
        self.w = WorkspaceClient()
        self.jobs = self.w.jobs
        
    def create_job(self, name: str, tasks: List[Dict]) -> str:
        """Create a new job with specified tasks."""
        job = self.jobs.create(
            name=name,
            tasks=tasks,
            email_notifications={
                "on_start": ["your-email@domain.com"],
                "on_success": ["your-email@domain.com"],
                "on_failure": ["your-email@domain.com"]
            },
            schedule={
                "quartz_cron_expression": "0 0 1 * * ?",  # Run daily at 1 AM
                "timezone_id": "UTC"
            }
        )
        return job.job_id

    def create_task(self, notebook_path: str, task_key: str, depends_on: List[str] = None) -> Dict:
        """Create a task configuration for a notebook."""
        return {
            "task_key": task_key,
            "notebook_task": {
                "notebook_path": notebook_path,
                "source": "WORKSPACE"
            },
            "depends_on": [{"task_key": dep} for dep in (depends_on or [])],
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 2,
                "spark_conf": {
                    "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
                    "spark.databricks.delta.autoCompact.enabled": "true",
                    "spark.databricks.delta.optimizeWrite.enabled": "true"
                }
            }
        }

    def create_pipeline_workflow(self):
        """Create the complete data pipeline workflow."""
        # Define tasks
        tasks = [
            # Bronze Layer
            self.create_task(
                notebook_path="/notebooks/bronze/source_data",
                task_key="process_source_data"
            ),
            
            # Silver Layer - Entity
            self.create_task(
                notebook_path="/notebooks/silver/entity",
                task_key="process_entity_data",
                depends_on=["process_source_data"]
            ),
            
            # Silver Layer - Fund
            self.create_task(
                notebook_path="/notebooks/silver/fund",
                task_key="process_fund_data",
                depends_on=["process_source_data"]
            )
        ]
        
        # Create the job
        job_id = self.create_job(
            name="Data Pipeline Workflow",
            tasks=tasks
        )
        
        logger.info(f"Created workflow job with ID: {job_id}")
        return job_id

    def run_job(self, job_id: str):
        """Run a job and wait for completion."""
        run = self.jobs.run_now(job_id=job_id)
        run_id = run.run_id
        
        while True:
            run_status = self.jobs.get_run(run_id=run_id)
            if run_status.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                break
            time.sleep(30)
        
        if run_status.state.result_state == "SUCCESS":
            logger.info(f"Job {job_id} completed successfully")
        else:
            logger.error(f"Job {job_id} failed with state: {run_status.state.result_state}")
            raise Exception(f"Job failed: {run_status.state.state_message}")

def main():
    """Main function to create and run the workflow."""
    try:
        orchestrator = JobOrchestrator()
        
        # Create the workflow
        job_id = orchestrator.create_pipeline_workflow()
        
        # Run the workflow
        orchestrator.run_job(job_id)
        
    except Exception as e:
        logger.error(f"Error in workflow execution: {str(e)}")
        raise

if __name__ == "__main__":
    main() 