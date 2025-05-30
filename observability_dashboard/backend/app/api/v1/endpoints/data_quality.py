from fastapi import APIRouter, Depends, HTTPException, Body, Query, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field # Ensure Field is imported
import logging

from app.db.session import get_db, SessionLocal # Import SessionLocal for background tasks
from app.services.data_quality_service import DataQualityService
from app.db.models import DataQualityMetric as ObsDataQualityMetricModel # SQLAlchemy model
from app.schemas import DataQualityMetric as DataQualityMetricSchema # Pydantic schema

router = APIRouter()
logger = logging.getLogger(__name__)

class RunChecksRequest(BaseModel):
    table_name: str = Field(..., example="my_data_table", description="Name of the table to check.")
    schema_name: Optional[str] = Field(None, example="public", description="Schema of the table. Defaults to database default if not provided.")
    columns_for_null_check: Optional[List[str]] = Field(None, example=["email", "phone_number"], description="List of columns to check for NULL values.")
    columns_for_duplicate_check: Optional[List[str]] = Field(None, example=["order_id"], description="List of columns to check for duplicate values.")
    run_row_count: bool = Field(True, description="Whether to perform a row count check.")
    dag_id: Optional[str] = Field(None, description="Optional DAG ID to associate with these checks.")
    task_id: Optional[str] = Field(None, description="Optional Task ID to associate with these checks.")

def run_dq_checks_background_task(obs_db_session_for_task: Session, request_data: RunChecksRequest):
    # This function runs in the background and needs its own DataQualityService instance
    # and to manage its own sessions carefully.
    logger.info(f"Background DQ task started for table: {request_data.schema_name or 'default'}.{request_data.table_name}")
    try:
        # The DataQualityService will create its own app_db session if ApplicationDBSessionLocal is configured
        with DataQualityService(obs_db=obs_db_session_for_task) as dq_service:
            if not dq_service.app_db: # Check if app_db session could be established
                logger.error("Background DQ Task: Application database connection not available in DataQualityService.")
                # Save a metric indicating this failure to the observability DB
                dq_service._save_metric(
                    dag_id=request_data.dag_id, task_id=request_data.task_id,
                    table_name=request_data.table_name, column_name=None,
                    metric_name="dq_service_health", metric_value=-1.0, # Using -1.0 to indicate error state
                    notes="Application DB connection failed or not configured for background DQ task."
                )
                return # Cannot proceed with application DB checks

            # Perform checks
            if request_data.run_row_count:
                dq_service.get_table_row_count(request_data.table_name, request_data.schema_name, request_data.dag_id, request_data.task_id)
            if request_data.columns_for_null_check:
                dq_service.get_null_counts(request_data.table_name, request_data.columns_for_null_check, request_data.schema_name, request_data.dag_id, request_data.task_id)
            if request_data.columns_for_duplicate_check:
                dq_service.get_duplicate_counts(request_data.table_name, request_data.columns_for_duplicate_check, request_data.schema_name, request_data.dag_id, request_data.task_id)
        
        logger.info(f"Background DQ checks completed for {request_data.table_name}.")
    except Exception as e:
        logger.error(f"Error in background DQ task for {request_data.table_name}: {e}", exc_info=True)
        # If an error occurs, the individual check methods in DataQualityService should ideally save their own error metrics.
        # A general error metric could be saved here if the service couldn't even start.
    finally:
        obs_db_session_for_task.close() # Ensure the observability DB session for the background task is closed.
        logger.debug("Observability DB session for background DQ task closed.")


@router.post("/run-checks", status_code=202, summary="Run data quality checks on a table (async in background)")
async def run_data_quality_checks_api(
    request_body: RunChecksRequest,
    background_tasks: BackgroundTasks,
    # db: Session = Depends(get_db) # This db session is for the current request, not for the background task
):
    logger.info(f"Received request to run DQ checks for table: {request_body.schema_name or 'default'}.{request_body.table_name}")

    # Perform a quick pre-check to see if Application DB is likely connectable
    # This uses a short-lived session for the check.
    obs_db_for_precheck = SessionLocal()
    try:
        with DataQualityService(obs_db=obs_db_for_precheck) as dq_service_check:
            if not dq_service_check.app_db:
                logger.warning("Pre-check: Application database connection not available or not configured properly in DataQualityService.")
                raise HTTPException(
                    status_code=503, 
                    detail="Application database not available. Configure APPLICATION_DATABASE_URL correctly (and ensure it's not the default placeholder)."
                )
            # Attempt a quick, harmless operation like listing schemas or a test query if essential
            # For now, just checking if app_db session was created is the main check
            logger.info("Pre-check: DataQualityService initialized with app_db session successfully.")
    except HTTPException: # Re-raise if it's the one from above
        raise
    except ConnectionRefusedError: # Specific error for connection refused
        logger.error("Pre-check: Application DB connection refused.")
        raise HTTPException(status_code=503, detail="Application DB connection refused. Check if the database is running and accessible.")
    except Exception as e: # Catch any other init errors
        logger.error(f"Pre-check for DataQualityService failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to initialize data quality service for pre-check: {str(e)}")
    finally:
        obs_db_for_precheck.close()
            
    # If pre-check passes, create a new observability DB session specifically for the background task
    obs_db_for_background = SessionLocal()
    background_tasks.add_task(run_dq_checks_background_task, obs_db_for_background, request_body)
       
    return {"message": "Data quality checks accepted and initiated in the background. Results will be stored once processing is complete."}


@router.get("/metrics", response_model=List[DataQualityMetricSchema], summary="Get stored data quality metrics")
async def get_data_quality_metrics(
    db: Session = Depends(get_db),
    table_name: Optional[str] = Query(None, description="Filter by table name (case-insensitive partial match)."),
    metric_name: Optional[str] = Query(None, description="Filter by metric name (case-insensitive partial match)."),
    dag_id: Optional[str] = Query(None, description="Filter by associated DAG ID."),
    task_id: Optional[str] = Query(None, description="Filter by associated Task ID."),
    page: int = Query(1, ge=1, description="Page number for pagination."),
    page_size: int = Query(50, ge=1, le=200, description="Number of items per page.")
):
    logger.info(f"Fetching DQ metrics: table={table_name}, metric={metric_name}, dag_id={dag_id}, page={page}, size={page_size}")
    query = db.query(ObsDataQualityMetricModel)
    if table_name:
        query = query.filter(ObsDataQualityMetricModel.table_name.ilike(f"%{table_name}%"))
    if metric_name:
        query = query.filter(ObsDataQualityMetricModel.metric_name.ilike(f"%{metric_name}%"))
    if dag_id:
        query = query.filter(ObsDataQualityMetricModel.dag_id == dag_id)
    if task_id:
        query = query.filter(ObsDataQualityMetricModel.task_id == task_id)
       
    metrics = query.order_by(ObsDataQualityMetricModel.scan_timestamp.desc()).offset((page - 1) * page_size).limit(page_size).all()
    return metrics

@router.get("/list-application-tables", summary="List tables and columns from the configured application database")
async def list_application_tables_api(
    schema_name: Optional[str] = Query(None, description="Schema to inspect. Defaults to the database's default schema if not provided."),
    db: Session = Depends(get_db) # obs_db session for DataQualityService initialization
):
    logger.info(f"Request to list application tables for schema: {schema_name or 'default'}")
    try:
        # DataQualityService will use its internally managed app_db session
        with DataQualityService(obs_db=db) as dq_service:
            if not dq_service.app_db: # Check if app_db session could be established
                 logger.warning("API: Application database connection not available in DataQualityService.")
                 raise HTTPException(
                    status_code=503, 
                    detail="Application database not available. Configure APPLICATION_DATABASE_URL correctly (and ensure it's not the default placeholder)."
                )
            
            tables_data = dq_service.list_tables_and_columns(schema_param=schema_name)
            if not tables_data and schema_name:
                 logger.info(f"No tables found for schema '{schema_name}' or schema does not exist/is empty.")
                 # Return empty dict with a note, or 404 if schema not found is preferred
            elif not tables_data:
                 logger.info(f"No tables found in default schema or schema could not be determined.")


            return tables_data
    except HTTPException: # Re-raise if it's the one from above
        raise
    except ConnectionRefusedError: # Specific error for connection refused
        logger.error("API list-application-tables: Application DB connection refused.")
        raise HTTPException(status_code=503, detail="Application DB connection refused. Check if the database is running and accessible.")
    except Exception as e:
        logger.error(f"Error listing application tables via API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list application tables: {str(e)}")
