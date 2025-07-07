from dataclasses import dataclass
from typing import List, Dict

@dataclass
class ClusterConfig:
    spark_version: str = "13.3.x-scala2.12"
    node_type_id: str = "Standard_DS3_v2"
    num_workers: int = 2
    spark_conf: Dict[str, str] = None

    def __post_init__(self):
        self.spark_conf = {
            "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.optimizeWrite.enabled": "true"
        }

@dataclass
class NotificationConfig:
    on_start: List[str]
    on_success: List[str]
    on_failure: List[str]

@dataclass
class ScheduleConfig:
    cron_expression: str = "0 0 1 * * ?"  # Daily at 1 AM
    timezone: str = "UTC"

@dataclass
class WorkflowConfig:
    name: str = "Data Pipeline Workflow"
    cluster: ClusterConfig = ClusterConfig()
    notifications: NotificationConfig = None
    schedule: ScheduleConfig = ScheduleConfig()

    def __post_init__(self):
        if self.notifications is None:
            self.notifications = NotificationConfig(
                on_start=["your-email@domain.com"],
                on_success=["your-email@domain.com"],
                on_failure=["your-email@domain.com"]
            )

# Default configuration
default_config = WorkflowConfig() 