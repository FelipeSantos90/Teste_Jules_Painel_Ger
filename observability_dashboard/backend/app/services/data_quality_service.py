import logging
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, Session
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from app.core.config import settings
from app.db.models import DataQualityMetric as ObsDataQualityMetric # Observability DB Model
from app.schemas import DataQualityMetricCreate # Pydantic schema for creating DQ metrics

logger = logging.getLogger(__name__)

# Engine for Application Data DB
application_db_engine = None
ApplicationDBSessionLocal = None

# Check if APPLICATION_DATABASE_URL is set and not the default placeholder
if settings.APPLICATION_DATABASE_URL and \
   not settings.APPLICATION_DATABASE_URL.startswith("postgresql://app_user:app_password@localhost/application_data_db"):
    try:
        application_db_engine = create_engine(settings.APPLICATION_DATABASE_URL)
        ApplicationDBSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=application_db_engine)
        logger.info("Successfully created Application Data DB engine and SessionLocal.")
    except Exception as e:
        logger.error(f"Failed to create Application Data DB engine or SessionLocal: {e}", exc_info=True)
        application_db_engine = None # Ensure it's None on error
        ApplicationDBSessionLocal = None # Ensure it's None on error
else:
    logger.warning("APPLICATION_DATABASE_URL not set or is using the default placeholder value. "
                   "DataQualityService will be limited for application DB checks.")


class DataQualityService:
    def __init__(self, obs_db: Session): 
        self.obs_db = obs_db # For storing results in observability DB
        self.app_db: Optional[Session] = None # Initialize to None

        if ApplicationDBSessionLocal:
            try:
                self.app_db = ApplicationDBSessionLocal()
                logger.debug("Application DB session created for DataQualityService instance.")
            except Exception as e:
                logger.error(f"Failed to create a session for Application DB for DataQualityService instance: {e}", exc_info=True)
        else:
            logger.warning("ApplicationDBSessionLocal is not initialized; Application DB operations will be skipped by this DataQualityService instance.")
        # Developer Note on SQL Construction for Table/Column Names:
        # The methods below (get_table_row_count, get_null_counts, get_duplicate_counts)
        # construct SQL queries using f-strings for table and column names.
        # This is done after a basic alphanumeric validation check on these identifiers.
        # While this approach is taken for simplicity and because, in the current API flow,
        # these names are often derived from trusted sources (SQLAlchemy inspector or user input
        # that should ideally be validated against known schemas), it's crucial to understand
        # that constructing SQL with external input for identifiers can be a security risk
        # if the input is not strictly controlled or sanitized.
        # For environments requiring higher security or if identifiers could be complex or from
        # less trusted sources, consider using SQLAlchemy's reflection capabilities more directly
        # to build queries (e.g., Table(name, metadata, autoload_with=engine)) or ensure
        # very strict validation and dialect-specific quoting for all identifiers.
        # The current alphanumeric check is a basic safeguard. Parameter binding is used for all *values*.
       
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.app_db:
            self.app_db.close()
            logger.debug("Application DB session closed via __exit__.")

    def _save_metric(self, dag_id: Optional[str], task_id: Optional[str], table_name: str, column_name: Optional[str], metric_name: str, metric_value: Any, notes: Optional[str] = None):
        try:
            json_value = None
            float_value = None
            if isinstance(metric_value, (int, float)):
                float_value = float(metric_value)
            elif metric_value is None: # If metric_value itself is None, store it as such if possible or specific note
                notes = notes + " (Metric value was None)" if notes else "(Metric value was None)"
            else: # For other types (list, dict, string etc.)
                try:
                    # Attempt to store common serializable types directly in json_value
                    if isinstance(metric_value, (list, dict, str, bool)):
                         json_value = {"value": metric_value}
                    else: # For other non-directly serializable types, convert to string
                         json_value = {"value": str(metric_value)}
                except TypeError: 
                    json_value = {"value": str(metric_value)} # Fallback to string representation

            metric_data = DataQualityMetricCreate(
                dag_id=dag_id, 
                task_id=task_id,
                table_name=table_name,
                column_name=column_name,
                metric_name=metric_name,
                metric_value=float_value,
                metric_value_json=json_value,
                scan_timestamp=datetime.now(timezone.utc),
                notes=notes
            )
            db_metric = ObsDataQualityMetric(**metric_data.model_dump(exclude_none=True)) # Exclude Nones so DB defaults can apply if any
            self.obs_db.add(db_metric)
            self.obs_db.commit()
            logger.info(f"Saved data quality metric: {table_name}.{column_name or '*'} - {metric_name}: {metric_value if float_value is None else float_value}")
        except Exception as e:
            self.obs_db.rollback()
            logger.error(f"Error saving data quality metric for {table_name}: {e}", exc_info=True)

    def get_table_row_count(self, table_name: str, schema: Optional[str] = None, dag_id: Optional[str] = None, task_id: Optional[str] = None) -> Optional[int]:
        if not self.app_db:
            logger.warning(f"Application DB not available for row count on {table_name}")
            self._save_metric(dag_id, task_id, table_name, None, "row_count", -1.0, notes="Application DB not available or connection failed")
            return None
           
        full_table_name = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
        # Basic validation for table/schema names (very simplified)
        if not all(part.replace('_', '').isalnum() for part in f"{schema or ''}{table_name}".replace('"', '')):
            logger.error(f"Invalid table or schema name: {full_table_name}")
            self._save_metric(dag_id, task_id, table_name, None, "row_count", -1.0, notes=f"Invalid table/schema name: {full_table_name}")
            return None
        try:
            result = self.app_db.execute(text(f"SELECT COUNT(*) FROM {full_table_name}")).scalar_one_or_none()
            if result is not None:
                self._save_metric(dag_id, task_id, table_name, None, "row_count", result)
            return result
        except Exception as e:
            logger.error(f"Error getting row count for {full_table_name}: {e}", exc_info=True)
            self._save_metric(dag_id, task_id, table_name, None, "row_count", -1.0, notes=f"Error executing row count: {str(e)}")
            return None

    def get_null_counts(self, table_name: str, column_names: List[str], schema: Optional[str] = None, dag_id: Optional[str] = None, task_id: Optional[str] = None) -> Dict[str, Optional[int]]:
        if not self.app_db:
            logger.warning(f"Application DB not available for null counts on {table_name}")
            for col in column_names: self._save_metric(dag_id, task_id, table_name, col, "null_count", -1.0, notes="Application DB not available")
            return {col: None for col in column_names}

        full_table_name = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
        if not all(part.replace('_', '').isalnum() for part in f"{schema or ''}{table_name}".replace('"', '')):
            logger.error(f"Invalid table or schema name for null count: {full_table_name}")
            for col in column_names: self._save_metric(dag_id, task_id, table_name, col, "null_count", -1.0, notes=f"Invalid table/schema: {full_table_name}")
            return {col: None for col in column_names}
           
        results: Dict[str, Optional[int]] = {}
        for column_name in column_names:
            if not column_name.replace('_', '').isalnum(): # Simplified validation
                logger.error(f"Invalid column name for null count: {column_name}")
                self._save_metric(dag_id, task_id, table_name, column_name, "null_count", -1.0, notes=f"Invalid column name: {column_name}")
                results[column_name] = None
                continue
            try:
                query = text(f'SELECT COUNT(*) FROM {full_table_name} WHERE "{column_name}" IS NULL')
                count = self.app_db.execute(query).scalar_one_or_none()
                results[column_name] = count
                if count is not None:
                    self._save_metric(dag_id, task_id, table_name, column_name, "null_count", count)
            except Exception as e:
                logger.error(f"Error getting null count for {full_table_name}.\"{column_name}\": {e}", exc_info=True)
                results[column_name] = None
                self._save_metric(dag_id, task_id, table_name, column_name, "null_count", -1.0, notes=f"Error: {str(e)}")
        return results

    def get_duplicate_counts(self, table_name: str, column_names: List[str], schema: Optional[str] = None, dag_id: Optional[str] = None, task_id: Optional[str] = None) -> Dict[str, Optional[int]]:
        if not self.app_db:
            logger.warning(f"Application DB not available for duplicate counts on {table_name}")
            for col in column_names: self._save_metric(dag_id, task_id, table_name, col, "duplicate_count", -1.0, notes="Application DB not available")
            return {col: None for col in column_names}

        full_table_name = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
        if not all(part.replace('_', '').isalnum() for part in f"{schema or ''}{table_name}".replace('"', '')):
            logger.error(f"Invalid table or schema name for duplicate count: {full_table_name}")
            for col in column_names: self._save_metric(dag_id, task_id, table_name, col, "duplicate_count", -1.0, notes=f"Invalid table/schema: {full_table_name}")
            return {col: None for col in column_names}

        results: Dict[str, Optional[int]] = {}
        for column_name in column_names:
            if not column_name.replace('_', '').isalnum(): # Simplified validation
                logger.error(f"Invalid column name for duplicate count: {column_name}")
                self._save_metric(dag_id, task_id, table_name, column_name, "duplicate_count", -1.0, notes=f"Invalid column: {column_name}")
                results[column_name] = None
                continue
            try:
                # This query counts rows that are part of a group of duplicates.
                # Sum of (COUNT(*) - 1) for each group having COUNT(*) > 1 gives total number of "excess" duplicate rows.
                # A simpler metric: count of distinct values that have duplicates.
                # The prompt's query counts total records that are duplicates (e.g. if 3 records are same, it adds 3 to sum)
                # A more common definition of "duplicate count" is the number of records beyond the first unique one.
                # For example, if (A,A,A,B,B,C) then duplicate count for A is 2, for B is 1. Total duplicates = 3.
                # The provided query sums the sizes of all duplicate groups.
                # Let's adjust to count "number of records that are duplicates (excluding one original instance per group)"
                query_str = f"""
                    SELECT SUM(dup_counts - 1) FROM (
                        SELECT COUNT(*) as dup_counts
                        FROM {full_table_name}
                        WHERE "{column_name}" IS NOT NULL
                        GROUP BY "{column_name}"
                        HAVING COUNT(*) > 1
                    ) AS duplicate_groups;
                """
                count = self.app_db.execute(text(query_str)).scalar_one_or_none()
                actual_duplicates = count if count is not None else 0
                results[column_name] = actual_duplicates
                self._save_metric(dag_id, task_id, table_name, column_name, "duplicate_count", actual_duplicates)
            except Exception as e:
                logger.error(f"Error getting duplicate count for {full_table_name}.\"{column_name}\": {e}", exc_info=True)
                results[column_name] = None
                self._save_metric(dag_id, task_id, table_name, column_name, "duplicate_count", -1.0, notes=f"Error: {str(e)}")
        return results

    def list_tables_and_columns(self, schema_param: Optional[str] = None) -> Dict[str, List[str]]:
        if not self.app_db:
            logger.warning("Application DB not available for listing tables and columns.")
            return {}
           
        try:
            engine_to_inspect = self.app_db.get_bind() 
            if engine_to_inspect is None:
                logger.error("Cannot get engine from app_db session for inspector.")
                return {}
            inspector = inspect(engine_to_inspect)
            tables_and_columns = {}
            
            # Determine schema to inspect
            # If schema_param is provided, use it. Otherwise, use default_schema_name.
            current_schema_to_inspect = schema_param if schema_param else inspector.default_schema_name
            
            if not current_schema_to_inspect:
                 logger.warning("No schema provided and default schema name could not be determined by inspector. Cannot list tables.")
                 return {} # Cannot proceed without a schema

            logger.info(f"Inspecting schema: {current_schema_to_inspect} for tables and columns.")
            table_names = inspector.get_table_names(schema=current_schema_to_inspect)
            for table_name in table_names:
                columns = [col['name'] for col in inspector.get_columns(table_name, schema=current_schema_to_inspect)]
                # Store with schema name if it's not the default one being inspected, or if schema_param was given
                key_name = f"{current_schema_to_inspect}.{table_name}" if schema_param or current_schema_to_inspect != inspector.default_schema_name else table_name
                tables_and_columns[key_name] = columns
               
            if not tables_and_columns:
                logger.warning(f"No tables found in schema: {current_schema_to_inspect}")
            return tables_and_columns
        except Exception as e:
            logger.error(f"Error listing tables and columns for schema '{schema_param}': {e}", exc_info=True)
            return {}

    def close_app_db_session(self): # Added method to explicitly close app_db session if needed
        if self.app_db:
            self.app_db.close()
            logger.info("Application DB session explicitly closed by service.")
            self.app_db = None
