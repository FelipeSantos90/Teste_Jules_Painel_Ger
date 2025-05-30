import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession # Renamed to avoid conflict
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone
import json # For handling 'conf' if it's a JSON string

# Application-specific imports
from app.db.models import DagRun as ObsDagRun, TaskInstance as ObsTaskInstance
from app.db.models import DagRunStatus as ObsDagRunStatusEnum, TaskInstanceStatus as ObsTaskInstanceStatusEnum # Enum imports
from app.schemas import DagRunCreate, TaskInstanceCreate # Pydantic Schemas
from app.core.config import settings

logger = logging.getLogger(__name__)

# Airflow DB Setup
airflow_engine = None
AirflowSessionLocal = None

if settings.AIRFLOW_DATABASE_URL:
    try:
        airflow_engine = create_engine(settings.AIRFLOW_DATABASE_URL)
        AirflowSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=airflow_engine)
        logger.info("Successfully created Airflow DB engine and SessionLocal.")
    except Exception as e:
        logger.error(f"Failed to create Airflow DB engine or SessionLocal: {e}", exc_info=True)
        # Keep airflow_engine and AirflowSessionLocal as None
else:
    logger.warning("AIRFLOW_DATABASE_URL is not set. AirflowService will be non-functional for DB sync.")


def map_airflow_dag_run_status(airflow_status: Optional[str]) -> ObsDagRunStatusEnum:
    if not airflow_status:
        return ObsDagRunStatusEnum.FAILED
    status_mapping = {
        "success": ObsDagRunStatusEnum.SUCCESS,
        "failed": ObsDagRunStatusEnum.FAILED,
        "running": ObsDagRunStatusEnum.RUNNING,
        "queued": ObsDagRunStatusEnum.RUNNING, # Consider 'queued' as 'running' from a high level
    }
    return status_mapping.get(airflow_status.lower(), ObsDagRunStatusEnum.FAILED) # Default to FAILED if unknown

def map_airflow_task_instance_status(airflow_status: Optional[str]) -> ObsTaskInstanceStatusEnum:
    if not airflow_status:
        return ObsTaskInstanceStatusEnum.FAILED
    status_mapping = {
        "success": ObsTaskInstanceStatusEnum.SUCCESS,
        "failed": ObsTaskInstanceStatusEnum.FAILED,
        "running": ObsTaskInstanceStatusEnum.RUNNING,
        "upstream_failed": ObsTaskInstanceStatusEnum.UPSTREAM_FAILED,
        "skipped": ObsTaskInstanceStatusEnum.SKIPPED,
        "removed": ObsTaskInstanceStatusEnum.REMOVED, # Should not typically occur for completed tasks
        "scheduled": ObsTaskInstanceStatusEnum.SCHEDULED,
        "queued": ObsTaskInstanceStatusEnum.SCHEDULED, # Tasks waiting in queue
        "up_for_retry": ObsTaskInstanceStatusEnum.RUNNING, # Task is attempting a retry
        "up_for_reschedule": ObsTaskInstanceStatusEnum.SCHEDULED,
        "deferred": ObsTaskInstanceStatusEnum.RUNNING, # Task is deferred, effectively still 'running'
        "sensing": ObsTaskInstanceStatusEnum.RUNNING, # Sensor waiting for condition
        "shutdown": ObsTaskInstanceStatusEnum.FAILED, # Task was externally stopped
        "restarting": ObsTaskInstanceStatusEnum.RUNNING, # Task is restarting
        "no_status": ObsTaskInstanceStatusEnum.FAILED, # No status usually means an issue
    }
    return status_mapping.get(airflow_status.lower(), ObsTaskInstanceStatusEnum.FAILED)


class AirflowService:
    def __init__(self, obs_db_session: SQLAlchemySession): # Type hint for observability DB session
        self.obs_db = obs_db_session
        self.airflow_db_session: Optional[SQLAlchemySession] = None

        if AirflowSessionLocal:
            try:
                self.airflow_db_session = AirflowSessionLocal()
                logger.debug("Airflow DB session created for AirflowService instance.")
            except Exception as e:
                logger.error(f"Failed to create a session for Airflow DB for AirflowService instance: {e}", exc_info=True)
        else:
            logger.warning("AirflowSessionLocal is not initialized; Airflow DB operations will be skipped.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.airflow_db_session:
            self.airflow_db_session.close()
            logger.debug("Airflow DB session closed via __exit__.")

    def _get_utc_aware_datetime(self, dt: Optional[datetime]) -> Optional[datetime]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _parse_conf(self, conf_val: Any) -> Optional[Dict[str, Any]]:
        if conf_val is None:
            return None
        if isinstance(conf_val, dict):
            return conf_val
        if isinstance(conf_val, str):
            try:
                # Handle potential byte string if conf is binary in Airflow DB (e.g., `bytearray`)
                if conf_val.startswith("b'") and conf_val.endswith("'"): # Crude check for byte string representation
                    conf_val = eval(conf_val) # eval `b'...'` into bytes
                if isinstance(conf_val, bytes):
                     conf_val = conf_val.decode('utf-8') # Decode bytes to string
                
                parsed_conf = json.loads(conf_val)
                if isinstance(parsed_conf, dict):
                    return parsed_conf
                else: # If JSON is not a dict (e.g. a list or scalar)
                    logger.warning(f"Parsed 'conf' is not a dictionary: {parsed_conf}. Storing as raw string under a key.")
                    return {"raw_conf": parsed_conf}
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse 'conf' JSON string: {conf_val}. Error: {e}. Storing as raw string.")
                return {"raw_conf": conf_val} # Store as is if parsing fails
            except Exception as e: # Catch other eval/decode errors
                logger.error(f"Unexpected error parsing 'conf': {conf_val}. Error: {e}", exc_info=True)
                return {"parsing_error": str(e), "original_conf": str(conf_val)}
        # For other types (e.g. already a bytearray if not handled by SQLAlchemy driver)
        if isinstance(conf_val, (bytes, bytearray)):
            try:
                decoded_conf = conf_val.decode('utf-8')
                parsed_conf = json.loads(decoded_conf)
                if isinstance(parsed_conf, dict):
                    return parsed_conf
                else:
                    logger.warning(f"Parsed decoded 'conf' is not a dictionary: {parsed_conf}. Storing as raw string.")
                    return {"raw_conf": parsed_conf}
            except Exception as e:
                logger.error(f"Error decoding/parsing byte-like 'conf': {e}", exc_info=True)
                return {"parsing_error": str(e), "original_conf_bytes": "omitted"}
        
        logger.warning(f"Unsupported type for 'conf': {type(conf_val)}. Value: {str(conf_val)[:100]}. Storing as string.")
        return {"unsupported_conf_type": str(type(conf_val)), "value_str": str(conf_val)}


    def sync_recent_dag_runs(self, lookback_period_hours: int = 72): # Extended default lookback
        if not self.airflow_db_session:
            logger.error("Airflow DB session not available. Skipping sync_recent_dag_runs.")
            return

        lookback_cutoff_aware = datetime.now(timezone.utc) - timedelta(hours=lookback_period_hours)
        # Airflow often stores naive UTC in `execution_date`
        naive_lookback_cutoff = lookback_cutoff_aware.replace(tzinfo=None)
        logger.info(f"Starting DAG run sync. Lookback: {lookback_period_hours} hrs (from {naive_lookback_cutoff} naive UTC).")

        try:
            # Assuming Airflow version >= 2.0
            # For Airflow < 2.0, `conf` might be a BLOB and needs `convert_from(conf, 'UTF8')` or similar in PostgreSQL
            # For modern Airflow, `conf` is often JSONB or TEXT that SQLAlchemy handles.
            # If `conf` is binary, specific casting might be needed: e.g. `CAST(conf AS TEXT)` for some DBs.
            dag_run_query = text("""
            SELECT dag_id, run_id, state, execution_date, start_date, end_date, external_trigger, conf
            FROM dag_run
            WHERE execution_date >= :lookback_cutoff 
            ORDER BY execution_date DESC
            """)
            
            airflow_runs_cursor = self.airflow_db_session.execute(dag_run_query, {'lookback_cutoff': naive_lookback_cutoff})
            airflow_runs = airflow_runs_cursor.fetchall() # Use .mappings().fetchall() for dict-like rows if preferred
            logger.info(f"Fetched {len(airflow_runs)} DAG runs from Airflow DB.")

            for raw_run in airflow_runs:
                run_id = raw_run.run_id
                dag_id = raw_run.dag_id
                
                execution_date = self._get_utc_aware_datetime(raw_run.execution_date)
                start_date = self._get_utc_aware_datetime(raw_run.start_date)
                end_date = self._get_utc_aware_datetime(raw_run.end_date)
                
                new_status = map_airflow_dag_run_status(raw_run.state)
                duration = (end_date - start_date).total_seconds() if start_date and end_date and end_date > start_date else None
                
                obs_dag_run = self.obs_db.query(ObsDagRun).filter(ObsDagRun.run_id == run_id).first()
                needs_task_sync = False

                current_time_utc = datetime.now(timezone.utc)

                if obs_dag_run:
                    # Check if significant fields have changed
                    if obs_dag_run.status != new_status or \
                       obs_dag_run.end_date != end_date or \
                       (obs_dag_run.start_date != start_date and start_date is not None): # Check start_date too
                        
                        logger.info(f"Updating DAG run: {dag_id} - {run_id} (Status: {obs_dag_run.status}->{new_status}, End: {obs_dag_run.end_date}->{end_date})")
                        obs_dag_run.status = new_status
                        obs_dag_run.start_date = start_date if start_date else obs_dag_run.start_date # Keep old if new is None
                        obs_dag_run.end_date = end_date
                        obs_dag_run.duration = duration
                        obs_dag_run.updated_at = current_time_utc
                        needs_task_sync = True # Status or end time changed, worth re-syncing tasks
                    
                    # If it was running and still is, or just finished, sync tasks
                    if obs_dag_run.status == ObsDagRunStatusEnum.RUNNING or \
                       (obs_dag_run.status != ObsDagRunStatusEnum.RUNNING and new_status != ObsDagRunStatusEnum.RUNNING and needs_task_sync): # Just finished
                        needs_task_sync = True

                else:
                    logger.info(f"Creating new DAG run record for: {dag_id} - {run_id}")
                    conf_data = self._parse_conf(raw_run.conf)
                    
                    new_dag_run_data = DagRunCreate(
                        dag_id=dag_id, run_id=run_id, status=new_status,
                        execution_date=execution_date, start_date=start_date, end_date=end_date,
                        external_trigger=bool(raw_run.external_trigger), conf=conf_data, duration=duration
                    )
                    obs_dag_run = ObsDagRun(**new_dag_run_data.model_dump())
                    obs_dag_run.created_at = current_time_utc # Set explicitly
                    obs_dag_run.updated_at = current_time_utc # Set explicitly
                    self.obs_db.add(obs_dag_run)
                    needs_task_sync = True
                
                try:
                    self.obs_db.commit()
                    if needs_task_sync and obs_dag_run: # Ensure obs_dag_run is not None
                        self.sync_task_instances_for_run(obs_dag_run.dag_id, obs_dag_run.run_id)
                except Exception as commit_e:
                    self.obs_db.rollback()
                    logger.error(f"Error committing DAG run {dag_id} - {run_id}: {commit_e}", exc_info=True)
                    # Potentially skip task sync for this run if commit failed

            logger.info("DAG run sync processing completed.")
        except Exception as e:
            if self.obs_db.is_active: # Check if transaction is active before rollback
                 self.obs_db.rollback()
            logger.error(f"Error during DAG run sync: {e}", exc_info=True)

    def sync_task_instances_for_run(self, dag_id: str, run_id: str):
        if not self.airflow_db_session:
            logger.error(f"Airflow DB session not available. Skipping task sync for {dag_id} - {run_id}.")
            return

        logger.info(f"Syncing task instances for DAG: {dag_id}, Run ID: {run_id}")
        try:
            # For Airflow, task_instance primary key is (dag_id, task_id, run_id, map_index)
            # try_number is usually not part of PK but indicates attempt. We care about the latest try for a given state.
            # The query should ideally fetch the latest try_number for each task_id within the run_id.
            # However, Airflow's task_instance table directly stores each try.
            # We will use try_number from Airflow, assuming it refers to the attempt number.
            # `log_url` is not directly in task_instance; it's often constructed or found elsewhere.
            task_instance_query = text("""
            SELECT task_id, try_number, state, start_date, end_date, operator, map_index
            FROM task_instance
            WHERE dag_id = :dag_id AND run_id = :run_id
            """)
            
            airflow_tasks_cursor = self.airflow_db_session.execute(task_instance_query, {'dag_id': dag_id, 'run_id': run_id})
            airflow_tasks = airflow_tasks_cursor.fetchall()
            logger.info(f"Fetched {len(airflow_tasks)} task instance attempts for {dag_id} - {run_id} from Airflow DB.")

            current_time_utc = datetime.now(timezone.utc)

            for raw_task in airflow_tasks:
                task_id = raw_task.task_id
                # Airflow map_index can be > 0 for mapped tasks. For non-mapped tasks it's -1 or 0.
                # If we need to distinguish mapped task instances, task_id might need to include map_index.
                # For now, assuming task_id from Airflow is unique enough for non-mapped, or context handles mapped.
                # If map_index is relevant, our ObsTaskInstance might need a map_index field.
                # Let's assume for now try_number is the key aspect for different attempts.
                
                effective_task_id = f"{raw_task.task_id}[{raw_task.map_index}]" if raw_task.map_index is not None and raw_task.map_index >= 0 else raw_task.task_id

                try_number = raw_task.try_number if raw_task.try_number is not None else 1
                
                start_date = self._get_utc_aware_datetime(raw_task.start_date)
                end_date = self._get_utc_aware_datetime(raw_task.end_date)

                new_status = map_airflow_task_instance_status(raw_task.state)
                duration = (end_date - start_date).total_seconds() if start_date and end_date and end_date > start_date else None

                # Composite key for task instance in our DB: (dag_id, task_id (effective), run_id, try_number)
                obs_task = self.obs_db.query(ObsTaskInstance).filter_by(
                    dag_id=dag_id, task_id=effective_task_id, run_id=run_id, try_number=try_number
                ).first()

                if obs_task:
                    if obs_task.status != new_status or obs_task.end_date != end_date or \
                       (obs_task.start_date != start_date and start_date is not None):
                        logger.info(f"Updating task: {effective_task_id} (try {try_number}) for run {run_id}")
                        obs_task.status = new_status
                        obs_task.start_date = start_date if start_date else obs_task.start_date
                        obs_task.end_date = end_date
                        obs_task.duration = duration
                        obs_task.updated_at = current_time_utc
                else:
                    logger.info(f"Creating new task instance record: {effective_task_id} (try {try_number}) for run {run_id}")
                    new_task_data = TaskInstanceCreate(
                        dag_id=dag_id, task_id=effective_task_id, run_id=run_id, try_number=try_number,
                        status=new_status, start_date=start_date, end_date=end_date, duration=duration,
                        # log_url: needs to be constructed if possible, or fetched via Airflow API if critical
                    )
                    obs_task_model = ObsTaskInstance(**new_task_data.model_dump())
                    obs_task_model.created_at = current_time_utc # Set explicitly
                    obs_task_model.updated_at = current_time_utc # Set explicitly
                    self.obs_db.add(obs_task_model)
            
            self.obs_db.commit()
            logger.info(f"Task instance sync completed for {dag_id} - {run_id}.")
        except Exception as e:
            if self.obs_db.is_active:
                self.obs_db.rollback()
            logger.error(f"Error during task instance sync for {dag_id} - {run_id}: {e}", exc_info=True)

    def close_airflow_db_session(self):
        if self.airflow_db_session:
            self.airflow_db_session.close()
            logger.info("Airflow DB session explicitly closed.")
            self.airflow_db_session = None

# Example usage (typically called from a background task or API endpoint)
# if __name__ == '__main__':
#     from app.db.session import SessionLocal # Observability DB session
#     obs_db = SessionLocal()
#     try:
#         with AirflowService(obs_db_session=obs_db) as airflow_service:
#             if airflow_service.airflow_db_session: # Check if session was successfully created
#                 airflow_service.sync_recent_dag_runs(lookback_period_hours=72)
#             else:
#                 logger.error("Cannot run sync: AirflowService could not connect to Airflow DB.")
#     finally:
#         obs_db.close()
#         if airflow_engine: # dispose engine if created
#             airflow_engine.dispose()

# Note: The __main__ block is for standalone testing; in FastAPI, session management is handled by dependencies.
# The AirflowService instance should be created per request or per task, managing its own Airflow DB session.
# The observability DB session (obs_db_session) is passed in.
