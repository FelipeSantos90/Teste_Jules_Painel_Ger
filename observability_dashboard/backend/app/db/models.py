from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, JSON, Enum as SAEnum, ForeignKey, Boolean
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func
import enum

Base = declarative_base()

class DagRunStatus(enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"

class TaskInstanceStatus(enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"
    REMOVED = "removed"
    SCHEDULED = "scheduled"

class DagRun(Base):
    __tablename__ = "dag_runs"

    id = Column(Integer, primary_key=True, index=True)
    dag_id = Column(String, nullable=False, index=True)
    run_id = Column(String, nullable=False, unique=True)
    status = Column(SAEnum(DagRunStatus), nullable=False)
    execution_date = Column(DateTime(timezone=True), nullable=False)
    start_date = Column(DateTime(timezone=True), server_default=func.now())
    end_date = Column(DateTime(timezone=True), nullable=True)
    duration = Column(Float, nullable=True) # in seconds
    external_trigger = Column(Boolean, default=False)
    conf = Column(JSON, nullable=True) # Airflow run configuration

    tasks = relationship("TaskInstance", back_populates="dag_run")

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class TaskInstance(Base):
    __tablename__ = "task_instances"

    id = Column(Integer, primary_key=True, index=True)
    dag_id = Column(String, nullable=False, index=True)
    task_id = Column(String, nullable=False, index=True)
    run_id = Column(String, ForeignKey("dag_runs.run_id"), nullable=False, index=True) # Links to DagRun's run_id, added index
    try_number = Column(Integer, default=1)
    status = Column(SAEnum(TaskInstanceStatus), nullable=False)
    start_date = Column(DateTime(timezone=True), nullable=True)
    end_date = Column(DateTime(timezone=True), nullable=True)
    duration = Column(Float, nullable=True) # in seconds
    log_url = Column(String, nullable=True) # Link to Airflow task log

    dag_run = relationship("DagRun", back_populates="tasks")

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class DataQualityMetric(Base):
    __tablename__ = "data_quality_metrics"

    id = Column(Integer, primary_key=True, index=True)
    dag_id = Column(String, nullable=True, index=True) # DAG that processed the data (can be NULL)
    task_id = Column(String, nullable=True, index=True) # Specific task related to this metric
    table_name = Column(String, nullable=False, index=True) # Table being analyzed
    column_name = Column(String, nullable=True, index=True) # Column being analyzed (if applicable)
    metric_name = Column(String, nullable=False) # e.g., 'null_count', 'duplicate_count', 'avg_value'
    metric_value = Column(Float, nullable=True)
    metric_value_json = Column(JSON, nullable=True) # For complex metrics like histograms or lists of outliers
    scan_timestamp = Column(DateTime(timezone=True), server_default=func.now())
    notes = Column(String, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
