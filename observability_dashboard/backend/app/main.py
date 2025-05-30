from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
from datetime import timezone # Added for timezone.utc

from app.api.v1.endpoints import jobs, dags, data_quality # Add this line
from app.db.session import SessionLocal # get_db is not directly used in main for startup
# Make sure to import airflow_engine from the service module if it's indeed defined there and needed globally
# For disposing, it might be better to manage engines more centrally if multiple are created.
# For now, assuming airflow_engine is correctly imported for disposal logic.
from app.services.airflow_service import AirflowService, airflow_engine as global_airflow_engine
from app.services.data_quality_service import application_db_engine as global_application_db_engine # For disposing
from app.core.config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO) # Ensure logger is configured to show info

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Application startup...")
    if settings.AIRFLOW_DATABASE_URL and settings.AIRFLOW_DATABASE_URL != "postgresql://airflow_user:airflow_password@localhost/airflow_db": # Check if it's configured and not the default placeholder
        logger.info("Attempting initial Airflow data sync as AIRFLOW_DATABASE_URL is set.")
        # Create a new session specifically for the startup task
        db_session_startup = SessionLocal()
        try:
            # AirflowService manages its own Airflow DB session internally
            # Pass the observability DB session
            airflow_service_instance = AirflowService(obs_db_session=db_session_startup)
            if airflow_service_instance.airflow_db_session: # Check if Airflow DB connection was successful
                # TODO: Consider making lookback configurable, or a different value for initial sync
                airflow_service_instance.sync_recent_dag_runs(lookback_period_hours=72)
                logger.info("Initial Airflow data sync process completed.")
                airflow_service_instance.close_airflow_db_session() # Clean up the session
            else:
                logger.warning("AirflowService could not establish a connection to the Airflow DB. Skipping initial sync.")
        except ConnectionError as ce: # More specific error type if possible from your DB driver
            logger.error(f"Startup Airflow Sync: Connection error to Airflow DB: {ce}", exc_info=True)
        except Exception as e:
            logger.error(f"Startup Airflow Sync: An unexpected error occurred: {e}", exc_info=True)
        finally:
            db_session_startup.close()
            logger.info("Observability DB session for startup sync closed.")
    else:
        logger.warning("AIRFLOW_DATABASE_URL not set or is default placeholder. Skipping initial Airflow data sync.")
    
    yield # Application runs here
    
    # Shutdown
    logger.info("Application shutdown...")
    if global_airflow_engine:
        logger.info("Disposing of global Airflow DB engine.")
        global_airflow_engine.dispose()
    if global_application_db_engine: # Dispose application DB engine as well
        logger.info("Disposing of global Application Data DB engine.")
        global_application_db_engine.dispose()


app = FastAPI(
    title="Observability Dashboard API",
    version="0.1.0",
    description="API for the Observability Dashboard, providing insights into DAG runs and job performance.",
    lifespan=lifespan
)

# Include routers with corrected prefixes
app.include_router(jobs.router, prefix="/api/v1/jobs", tags=["Jobs & Run History"])
app.include_router(dags.router, prefix="/api/v1/dags", tags=["DAGs Performance & Info"])
app.include_router(data_quality.router, prefix="/api/v1/data-quality", tags=["Data Quality"]) # Add this line

@app.get("/", tags=["Root"])
async def root():
    return {"message": "Welcome to the Observability Dashboard API. See /docs for available endpoints."}

# Note: The get_db dependency for FastAPI endpoints will handle session creation and teardown per request.
# The startup logic needs its own explicit session management as shown.
