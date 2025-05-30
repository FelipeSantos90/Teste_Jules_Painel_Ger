from pydantic import BaseModel, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime
import enum

# Enum for status, mirroring SQLAlchemy enums for consistency in API
class DagRunStatus(str, enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"

class TaskInstanceStatus(str, enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"
    REMOVED = "removed"
    SCHEDULED = "scheduled"

# Base Pydantic model for common fields
class BaseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

# TaskInstance Schemas
class TaskInstanceBase(BaseSchema):
    dag_id: str
    task_id: str
    run_id: str # Corresponds to DagRun.run_id
    try_number: Optional[int] = 1
    status: TaskInstanceStatus
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    duration: Optional[float] = None
    log_url: Optional[str] = None

class TaskInstanceCreate(TaskInstanceBase):
    pass

class TaskInstanceUpdate(BaseSchema): # For partial updates
    status: Optional[TaskInstanceStatus] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    duration: Optional[float] = None
    log_url: Optional[str] = None
    try_number: Optional[int] = None

class TaskInstance(TaskInstanceBase):
    id: int
    created_at: datetime
    updated_at: datetime

# DagRun Schemas
class DagRunBase(BaseSchema):
    dag_id: str
    run_id: str
    status: DagRunStatus
    execution_date: datetime
    start_date: Optional[datetime] = None # Can be null if not yet started
    end_date: Optional[datetime] = None
    duration: Optional[float] = None
    external_trigger: Optional[bool] = False
    conf: Optional[Dict[str, Any]] = None

class DagRunCreate(DagRunBase):
    # Potentially add tasks to be created simultaneously if needed
    pass

class DagRunUpdate(BaseSchema): # For partial updates
    status: Optional[DagRunStatus] = None
    end_date: Optional[datetime] = None
    duration: Optional[float] = None
    # run_id and execution_date are usually not updatable for an existing run

class DagRun(DagRunBase): # Schema for reading a DagRun
    id: int
    # tasks: List[TaskInstance] = [] # Removed default to avoid loading all tasks unless specified
    created_at: datetime
    updated_at: datetime

class DagRunWithTasks(DagRun): # Specific schema when tasks are included
    tasks: List[TaskInstance] = []


# DataQualityMetric Schemas
class DataQualityMetricBase(BaseSchema):
    dag_id: Optional[str] = None # Made optional to align with model
    task_id: Optional[str] = None
    table_name: str
    column_name: Optional[str] = None
    metric_name: str
    metric_value: Optional[float] = None
    metric_value_json: Optional[Dict[str, Any]] = None # Changed from List to Dict for better JSON structure
    scan_timestamp: Optional[datetime] = None # Defaults to now in model
    notes: Optional[str] = None

class DataQualityMetricCreate(DataQualityMetricBase):
    pass

class DataQualityMetric(DataQualityMetricBase):
    id: int
    created_at: datetime

# Schemas for API responses (examples)
class JobStats(BaseSchema): # This was an example, might not directly map to a model
    total_jobs: int
    successful_jobs: int
    failed_jobs: int
    running_jobs: int

class DagPerformance(BaseSchema): # This was an example, might not directly map to a model
    dag_id: str
    avg_duration: Optional[float] = None
    min_duration: Optional[float] = None
    max_duration: Optional[float] = None
    runs_analyzed: int
    success_rate: Optional[float] = None
