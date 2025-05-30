from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, case, literal_column
from typing import List, Optional
from datetime import datetime, timedelta, timezone

from app.db.session import get_db
from app.db.models import DagRun as ObsDagRunModel, TaskInstance as ObsTaskInstanceModel
from app.db.models import DagRunStatus as ObsDagRunStatusEnum # Enum for status
from app.schemas import DagPerformance, DagRun as DagRunSchema, TaskInstance as TaskInstanceSchema # Pydantic Schemas
# If a DagRun schema without tasks is needed, it should be defined, e.g., DagRunBasic.
# For now, using DagRunSchema and relying on SQLAlchemy's lazy loading for tasks.
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

def _get_query_times(start_time: Optional[datetime], end_time: Optional[datetime], default_timedelta_days: int = 7) -> (datetime, datetime):
    """Helper to manage timezone-aware default times for queries."""
    if end_time is None:
        end_time = datetime.now(timezone.utc)
    elif end_time.tzinfo is None: # Ensure end_time is aware
        end_time = end_time.replace(tzinfo=timezone.utc)

    if start_time is None:
        start_time = end_time - timedelta(days=default_timedelta_days)
    elif start_time.tzinfo is None: # Ensure start_time is aware
        start_time = start_time.replace(tzinfo=timezone.utc)
    return start_time, end_time

@router.get("/list", response_model=List[str])
def get_distinct_dag_ids(
    db: Session = Depends(get_db),
    limit: Optional[int] = Query(100, ge=1, le=1000, description="Limit the number of distinct DAG IDs returned."),
    # Optional: add search/filter if the list of DAGs becomes very large
    # search_term: Optional[str] = Query(None, description="Filter DAG IDs containing this term.")
):
    """
    Lists all unique DAG IDs present in the observability database.
    """
    logger.info(f"Fetching distinct DAG IDs with limit: {limit}")
    try:
        query = db.query(ObsDagRunModel.dag_id).distinct().order_by(ObsDagRunModel.dag_id)
        # if search_term:
        # query = query.filter(ObsDagRunModel.dag_id.ilike(f"%{search_term}%"))
            
        dag_ids = [row[0] for row in query.limit(limit).all()]
        return dag_ids
    except Exception as e:
        logger.error(f"Error fetching distinct DAG IDs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching distinct DAG IDs.")


@router.get("/performance", response_model=List[DagPerformance])
def get_dags_performance(
    db: Session = Depends(get_db),
    dag_id_filter: Optional[str] = Query(None, alias="dag_id", description="Filter by a specific DAG ID (exact match)."),
    start_time_query: Optional[datetime] = Query(None, description="Consider runs with execution_date on or after this time (ISO 8601). Defaults to 7 days ago.", alias="start_time"),
    end_time_query: Optional[datetime] = Query(None, description="Consider runs with execution_date on or before this time (ISO 8601). Defaults to now.", alias="end_time")
):
    start_time, end_time = _get_query_times(start_time_query, end_time_query, default_timedelta_days=7)
    logger.info(f"Fetching DAG performance for period: {start_time} to {end_time}, DAG ID: {dag_id_filter or 'all'}")

    try:
        # Query for average, min, max duration, and success rate of DAG runs
        query = db.query(
            ObsDagRunModel.dag_id,
            func.avg(ObsDagRunModel.duration).label("avg_duration"),
            func.min(ObsDagRunModel.duration).label("min_duration"),
            func.max(ObsDagRunModel.duration).label("max_duration"),
            func.count(ObsDagRunModel.id).label("total_runs"),
            func.sum(case((ObsDagRunModel.status == ObsDagRunStatusEnum.SUCCESS, 1), else_=0)).label("successful_runs")
        ).filter(
            # ObsDagRunModel.status == ObsDagRunStatusEnum.SUCCESS, # Only successful runs for duration metrics
            ObsDagRunModel.duration.isnot(None), # Ensure duration is not null for these stats
            ObsDagRunModel.execution_date >= start_time,
            ObsDagRunModel.execution_date <= end_time
        ).group_by(ObsDagRunModel.dag_id).order_by(ObsDagRunModel.dag_id)

        if dag_id_filter:
            query = query.filter(ObsDagRunModel.dag_id == dag_id_filter)
            
        results = query.all()
        
        performance_data = []
        for r in results:
            success_rate = (r.successful_runs / r.total_runs) * 100 if r.total_runs > 0 else 0
            # Filter for duration stats only on successful runs if preferred,
            # For now, avg/min/max duration is calculated on runs that have a duration (typically completed runs)
            # and success_rate is calculated over all runs in the period.
            # If avg_duration should only be for successful runs, the query needs adjustment or post-processing.
            # Let's refine to calculate duration metrics ONLY on successful runs.
            
            # Refined query for duration metrics on successful runs only for this DAG_ID
            duration_stats_q = db.query(
                func.avg(ObsDagRunModel.duration).label("avg_s_duration"),
                func.min(ObsDagRunModel.duration).label("min_s_duration"),
                func.max(ObsDagRunModel.duration).label("max_s_duration")
            ).filter(
                ObsDagRunModel.dag_id == r.dag_id,
                ObsDagRunModel.status == ObsDagRunStatusEnum.SUCCESS,
                ObsDagRunModel.duration.isnot(None),
                ObsDagRunModel.execution_date >= start_time,
                ObsDagRunModel.execution_date <= end_time
            ).first()

            performance_data.append(
                DagPerformance(
                    dag_id=r.dag_id,
                    avg_duration=duration_stats_q.avg_s_duration if duration_stats_q else None,
                    min_duration=duration_stats_q.min_s_duration if duration_stats_q else None,
                    max_duration=duration_stats_q.max_s_duration if duration_stats_q else None,
                    runs_analyzed=r.total_runs, # Total runs in the period (could be all statuses)
                    success_rate=success_rate
                )
            )
        return performance_data
    except Exception as e:
        logger.error(f"Error fetching DAG performance: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching DAG performance: {str(e)}")

@router.get("/{dag_id}/runs", response_model=List[DagRunSchema])
def get_dag_runs_for_dag(
    dag_id: str,
    db: Session = Depends(get_db),
    status_filter: Optional[ObsDagRunStatusEnum] = Query(None, alias="status", description="Filter by DAG run status."),
    page: int = Query(1, ge=1, description="Page number for pagination."),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page."),
    start_time_query: Optional[datetime] = Query(None, description="Filter execution_date on or after (ISO 8601). Defaults to 30 days ago.", alias="start_time"),
    end_time_query: Optional[datetime] = Query(None, description="Filter execution_date on or before (ISO 8601). Defaults to now.", alias="end_time"),
    sort_by: str = Query("execution_date", description="Field to sort by."),
    sort_order: str = Query("desc", description="Sort order: 'asc' or 'desc'.")
):
    """Gets the run history for a specific DAG ID."""
    start_time, end_time = _get_query_times(start_time_query, end_time_query, default_timedelta_days=30)
    logger.info(f"Fetching runs for DAG ID: {dag_id}, page={page}, size={page_size}, status={status_filter}, period=({start_time} to {end_time})")

    query = db.query(ObsDagRunModel).filter(ObsDagRunModel.dag_id == dag_id)

    if status_filter:
        query = query.filter(ObsDagRunModel.status == status_filter)
    
    query = query.filter(ObsDagRunModel.execution_date >= start_time, ObsDagRunModel.execution_date <= end_time)
        
    sort_column = getattr(ObsDagRunModel, sort_by, ObsDagRunModel.execution_date)
    if sort_order.lower() == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc())
            
    dag_runs = query.offset((page - 1) * page_size).limit(page_size).all()
    return dag_runs


@router.get("/{dag_id}/runs/{run_id}/tasks", response_model=List[TaskInstanceSchema])
def get_task_instances_for_dag_run(
    dag_id: str, # From path
    run_id: str, # From path
    db: Session = Depends(get_db),
    status_filter: Optional[str] = Query(None, alias="status", description="Filter tasks by status."),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200) # Tasks can be numerous
):
    """Gets all task instances for a specific DAG run."""
    logger.info(f"Fetching task instances for DAG ID: {dag_id}, Run ID: {run_id}, page={page}, size={page_size}, status={status_filter}")
    
    # First, verify the DagRun exists
    dag_run = db.query(ObsDagRunModel.id).filter(ObsDagRunModel.dag_id == dag_id, ObsDagRunModel.run_id == run_id).first()
    if not dag_run:
        raise HTTPException(status_code=404, detail=f"DAG run not found for DAG ID '{dag_id}' and Run ID '{run_id}'")

    query = db.query(ObsTaskInstanceModel).filter(
        ObsTaskInstanceModel.dag_id == dag_id,
        ObsTaskInstanceModel.run_id == run_id # This is the crucial link
    )

    if status_filter:
        query = query.filter(ObsTaskInstanceModel.status == status_filter)
    
    query = query.order_by(ObsTaskInstanceModel.start_date.asc(), ObsTaskInstanceModel.task_id.asc()) # Default sort
    
    task_instances = query.offset((page - 1) * page_size).limit(page_size).all()
    return task_instances

# Placeholder for Data Quality endpoint (to be implemented in a later step)
# @router.get("/{dag_id}/data-quality-metrics")
# async def get_data_quality_for_dag(dag_id: str, db: Session = Depends(get_db)):
#     # This would fetch from DataQualityMetric model
#     return {"message": f"Data quality metrics for {dag_id} will be available here."}
