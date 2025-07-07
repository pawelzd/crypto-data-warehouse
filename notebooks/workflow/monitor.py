from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import List, Dict
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkflowMonitor:
    def __init__(self):
        self.w = WorkspaceClient()
        self.jobs = self.w.jobs
        
    def get_job_runs(self, job_id: str, days_back: int = 7) -> pd.DataFrame:
        """Get job runs for the specified job ID."""
        start_time = datetime.now() - timedelta(days=days_back)
        
        runs = self.jobs.list_runs(
            job_id=job_id,
            start_time_from=start_time.timestamp() * 1000
        )
        
        # Convert runs to DataFrame
        runs_data = []
        for run in runs:
            runs_data.append({
                'run_id': run.run_id,
                'start_time': datetime.fromtimestamp(run.start_time/1000),
                'end_time': datetime.fromtimestamp(run.end_time/1000) if run.end_time else None,
                'state': run.state.life_cycle_state,
                'result_state': run.state.result_state,
                'duration': (run.end_time - run.start_time)/1000 if run.end_time else None,
                'error_message': run.state.state_message if run.state.result_state == 'FAILED' else None
            })
        
        return pd.DataFrame(runs_data)
    
    def get_task_runs(self, run_id: str) -> pd.DataFrame:
        """Get task runs for a specific job run."""
        tasks = self.jobs.list_run_outputs(run_id=run_id)
        
        task_data = []
        for task in tasks:
            task_data.append({
                'task_key': task.task_key,
                'state': task.state.life_cycle_state,
                'result_state': task.state.result_state,
                'start_time': datetime.fromtimestamp(task.start_time/1000),
                'end_time': datetime.fromtimestamp(task.end_time/1000) if task.end_time else None,
                'duration': (task.end_time - task.start_time)/1000 if task.end_time else None
            })
        
        return pd.DataFrame(task_data)
    
    def analyze_workflow_performance(self, job_id: str, days_back: int = 7) -> Dict:
        """Analyze workflow performance metrics."""
        runs_df = self.get_job_runs(job_id, days_back)
        
        # Calculate metrics
        total_runs = len(runs_df)
        successful_runs = len(runs_df[runs_df['result_state'] == 'SUCCESS'])
        failed_runs = len(runs_df[runs_df['result_state'] == 'FAILED'])
        
        avg_duration = runs_df['duration'].mean() if not runs_df.empty else 0
        
        return {
            'total_runs': total_runs,
            'successful_runs': successful_runs,
            'failed_runs': failed_runs,
            'success_rate': (successful_runs / total_runs * 100) if total_runs > 0 else 0,
            'average_duration_seconds': avg_duration,
            'last_run_status': runs_df.iloc[0]['result_state'] if not runs_df.empty else None,
            'last_run_time': runs_df.iloc[0]['start_time'] if not runs_df.empty else None
        }

def main():
    """Main function to monitor workflow execution."""
    try:
        monitor = WorkflowMonitor()
        
        # Replace with your job ID
        job_id = "your-job-id"
        
        # Get recent runs
        runs_df = monitor.get_job_runs(job_id)
        print("\nRecent Job Runs:")
        print(runs_df)
        
        # Get latest run tasks
        if not runs_df.empty:
            latest_run_id = runs_df.iloc[0]['run_id']
            tasks_df = monitor.get_task_runs(latest_run_id)
            print("\nLatest Run Tasks:")
            print(tasks_df)
        
        # Analyze performance
        performance = monitor.analyze_workflow_performance(job_id)
        print("\nPerformance Analysis:")
        print(json.dumps(performance, indent=2))
        
    except Exception as e:
        logger.error(f"Error in workflow monitoring: {str(e)}")
        raise

if __name__ == "__main__":
    main() 