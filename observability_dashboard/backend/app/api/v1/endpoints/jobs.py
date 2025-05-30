from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func # For count
from typing import List, Optional
from datetime import datetime, timedelta, timezone # Ensure timezone is imported

from app.db.session import get_db
from app.db.models import DagRun as ObsDagRunModel # Renamed to avoid conflict with schema
from app.db.models import DagRunStatus as ObsDagRunStatusEnum # Enum for status
from app.schemas import JobStats, DagRun as DagRunSchema # Pydantic Schemas
from app.services.airflow_service import AirflowService # For manual sync trigger
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

def _get_query_times(start_time: Optional[datetime], end_time: Optional[datetime]) -> (datetime, datetime):
    """Helper to manage timezone-aware default times for queries."""
    if end_time is None:
        end_time = datetime.now(timezone.utc)
    elif end_time.tzinfo is None: # Ensure end_time is aware
        end_time = end_time.replace(tzinfo=timezone.utc)

    if start_time is None:
        start_time = end_time - timedelta(days=1)
    elif start_time.tzinfo is None: # Ensure start_time is aware
        start_time = start_time.replace(tzinfo=timezone.utc)
    return start_time, end_time

@router.get("/status", response_model=JobStats)
def get_job_status(
    db: Session = Depends(get_db),
    start_time_query: Optional[datetime] = Query(None, description="Start of time window (ISO 8601). Defaults to 24 hours ago.", alias="start_time"),
    end_time_query: Optional[datetime] = Query(None, description="End of time window (ISO 8601). Defaults to now.", alias="end_time")
):
    start_time, end_time = _get_query_times(start_time_query, end_time_query)
    logger.info(f"Fetching job status for period: {start_time} to {end_time}")

    try:
        # Base query filtered by the time window using execution_date
        base_query = db.query(ObsDagRunModel).filter(
            ObsDagRunModel.execution_date >= start_time, 
            ObsDagRunModel.execution_date <= end_time
        )
        
        total_jobs = base_query.count()
        successful_jobs = base_query.filter(ObsDagRunModel.status == ObsDagRunStatusEnum.SUCCESS).count()
        failed_jobs = base_query.filter(ObsDagRunModel.status == ObsDagRunStatusEnum.FAILED).count()
        running_jobs = base_query.filter(ObsDagRunModel.status == ObsDagRunStatusEnum.RUNNING).count()
        
        return JobStats(
            total_jobs=total_jobs,
            successful_jobs=successful_jobs,
            failed_jobs=failed_jobs,
            running_jobs=running_jobs
        )
    except Exception as e:
        logger.error(f"Error fetching job status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching job status: {str(e)}")


@router.get("/history", response_model=List[DagRunSchema])
def get_jobs_history(
    db: Session = Depends(get_db),
    dag_id_filter: Optional[str] = Query(None, alias="dag_id", description="Filter by DAG ID (exact match)."),
    status_filter: Optional[ObsDagRunStatusEnum] = Query(None, alias="status", description="Filter by DAG run status."),
    page: int = Query(1, ge=1, description="Page number for pagination."),
    page_size: int = Query(20, ge=1, le=200, description="Number of items per page (max 200)."),
    start_time_query: Optional[datetime] = Query(None, description="Filter for execution_date on or after (ISO 8601).", alias="start_time"),
    end_time_query: Optional[datetime] = Query(None, description="Filter for execution_date on or before (ISO 8601).", alias="end_time"),
    sort_by: str = Query("execution_date", description="Field to sort by (e.g., execution_date, start_date, status)."),
    sort_order: str = Query("desc", description="Sort order: 'asc' or 'desc'.")
):
    start_time, end_time = _get_query_times(start_time_query, end_time_query)
    logger.info(f"Fetching job history: page={page}, size={page_size}, dag_id={dag_id_filter}, status={status_filter}, period=({start_time} to {end_time})")

    query = db.query(ObsDagRunModel)

    if dag_id_filter:
        query = query.filter(ObsDagRunModel.dag_id.ilike(f"%{dag_id_filter}%")) # Use ilike for case-insensitive partial match
    if status_filter:
        query = query.filter(ObsDagRunModel.status == status_filter)
    
    # Apply time window filter
    query = query.filter(ObsDagRunModel.execution_date >= start_time, ObsDagRunModel.execution_date <= end_time)
        
    # Sorting
    sort_column = getattr(ObsDagRunModel, sort_by, ObsDagRunModel.execution_date) # Default to execution_date
    if sort_order.lower() == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc())
    
    # total_items = query.count() # This would be total after filtering
    
    dag_runs = query.offset((page - 1) * page_size).limit(page_size).all()
    
    # The DagRunSchema in schemas/__init__.py includes `tasks: List[TaskInstance] = []`
    # By default, if lazy loading is enabled (SQLAlchemy default), tasks won't be fetched unless accessed.
    # This is generally efficient for a history view.
    # If tasks were eagerly loaded by default, a different schema (e.g., DagRunBasic) would be better here.
    return dag_runs

def run_airflow_sync_background(db_session: Session, lookback_hours: int):
    try:
        logger.info(f"Background Airflow sync task started. Lookback: {lookback_hours} hours.")
        # AirflowService manages its own Airflow DB session
        airflow_service = AirflowService(obs_db_session=db_session)
        if airflow_service.airflow_db_session:
            airflow_service.sync_recent_dag_runs(lookback_period_hours=lookback_hours)
            airflow_service.close_airflow_db_session()
            logger.info("Background Airflow sync task completed.")
        else:
            logger.warning("Background Airflow sync task: Airflow DB session not available.")
    except ConnectionError as ce:
        logger.error(f"Background Airflow Sync: Connection error: {ce}", exc_info=True)
    except Exception as e:
        logger.error(f"Background Airflow Sync: An error occurred: {e}", exc_info=True)
    finally:
        db_session.close() # Close the observability DB session passed to this background task

@router.post("/trigger-airflow-sync", status_code=202)
async def trigger_sync(
    background_tasks: BackgroundTasks,
    lookback_hours: Optional[int] = Query(24, description="Number of hours to look back for DAG runs."),
    # We need a new DB session for the background task
    # Cannot use Depends(get_db) directly in the function called by background_tasks
):
    logger.info(f"Received request to trigger Airflow sync with lookback: {lookback_hours} hours.")
    # Create a new session for the background task
    # This is crucial because the session from Depends(get_db) is tied to the request lifecycle
    db_session_for_bg = SessionLocal() # Assuming SessionLocal is your factory from db.session
    
    background_tasks.add_task(run_airflow_sync_background, db_session_for_bg, lookback_hours)
    return {"message": "Airflow data synchronization task accepted and will run in the background."}
