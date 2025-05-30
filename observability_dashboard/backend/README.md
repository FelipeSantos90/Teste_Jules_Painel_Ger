# Observability Dashboard Backend

This is the backend for the Observability Dashboard, built with FastAPI.

## Setup and Running

1.  **Prerequisites:**
    *   Python 3.9+
    *   Poetry (for managing dependencies, as defined in `pyproject.toml`)
    *   A running PostgreSQL instance (or adapt `DATABASE_URL` in `.env` and `alembic.ini` for another DB).
    *   Optionally, a running Airflow instance with its database accessible if you want to sync Airflow metadata (`AIRFLOW_DATABASE_URL`).
    *   Optionally, an application database if you want to use the Data Quality service (`APPLICATION_DATABASE_URL`).

2.  **Installation:**
    *   Navigate to the `observability_dashboard/backend` directory.
    *   Create a virtual environment: `python -m venv .venv` (or `python3 -m venv .venv`)
    *   Activate it: `source .venv/bin/activate` (Linux/macOS) or `.venv\Scripts\activate` (Windows)
    *   Install dependencies: `poetry install`

3.  **Environment Variables:**
    *   Create a `.env` file in the `observability_dashboard/backend` directory (this file is gitignored).
    *   Add the following variables (adjust as necessary):
        ```env
        DATABASE_URL="postgresql://your_obs_user:your_obs_password@localhost:5432/observability_db"
        
        # For Airflow metadata synchronization (optional)
        AIRFLOW_DATABASE_URL="postgresql://your_airflow_user:your_airflow_password@localhost:5432/airflow_db"
        # AIRFLOW_API_URL="http://localhost:8080/api/v1" # If using Airflow API features

        # For Data Quality service connecting to an application database (optional)
        APPLICATION_DATABASE_URL="postgresql://your_app_user:your_app_password@localhost:5432/your_application_db"
        ```
    *   Refer to `app/core/config.py` for all possible environment variables.

4.  **Database Migrations (Alembic for Observability DB):**
    *   Ensure `sqlalchemy.url` in `alembic.ini` is correctly pointing to your observability database. You can set it directly or modify `alembic.ini` to load it from an environment variable like `%(OBSERVABILITY_DB_URL)s` and then set that variable. For simplicity, you might temporarily set it directly in `alembic.ini` if you're not using environment variable substitution there.
        *Example `alembic.ini` line:* `sqlalchemy.url = postgresql://your_obs_user:your_obs_password@localhost:5432/observability_db`
    *   Modify `alembic/env.py` to import your SQLAlchemy models for the observability database:
        ```python
        # In alembic/env.py, around line 20-25:
        # import sys, os # Ensure os is imported
        # sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))) # Add this line to ensure 'app' is discoverable
        # from app.db.models import Base # Add this line, assuming your models.py has Base for observability DB
        # target_metadata = Base.metadata # Modify this line to use your Base
        ```
    *   Create initial migration (if you have models defined for observability DB): `poetry run alembic revision -m "initial schema"`
    *   Apply migrations: `poetry run alembic upgrade head`

5.  **Running the application:**
    *   `poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`
    *   The API will be available at `http://localhost:8000`.
    *   Swagger UI documentation at `http://localhost:8000/docs`.

## Project Structure

*   `app/`: Main application code.
    *   `main.py`: FastAPI app definition and root endpoints.
    *   `api/`: API endpoint routers.
    *   `core/`: Configuration and settings (`config.py`).
    *   `db/`: Database models (`models.py`) and session management (`session.py`) for the observability database.
    *   `schemas/`: Pydantic schemas for data validation and serialization.
    *   `services/`: Business logic (e.g., `airflow_service.py`, `data_quality_service.py`).
*   `tests/`: Unit and integration tests (to be developed).
*   `alembic/`: Database migration scripts for the observability database.
*   `alembic.ini`: Alembic configuration.
*   `pyproject.toml`: Project metadata and dependencies (Poetry).
*   `.env` (you create this): Environment variable storage for local development.

## Security Considerations

### Secrets Management
All sensitive configuration values, such as database connection strings (`DATABASE_URL`, `AIRFLOW_DATABASE_URL`, `APPLICATION_DATABASE_URL`), API keys, or other secrets, **must** be managed via environment variables.
- For local development, you can use a `.env` file (gitignored by default) in the `backend` directory.
- In production environments, these should be set as actual environment variables in your deployment environment (e.g., Docker environment variables, server configurations, CI/CD pipeline secrets).
- **Do not hardcode secrets directly in the source code.** The `app/core/config.py` file uses `pydantic-settings` to load these from the environment.

### Input Validation
- The API uses FastAPI's Pydantic integration for automatic request body validation.
- Path and query parameters are also validated based on Python type hints.
- For data used in raw SQL queries (e.g., table/column names in Data Quality service), ensure robust validation and sanitization is performed. Current implementation uses basic alphanumeric checks and relies on SQLAlchemy's parameter binding for values. Table/column identifiers used in f-strings are a known sensitive area; see notes in `DataQualityService`.

### SQL Injection
- SQLAlchemy ORM and parameterized queries (using `text()` with bound parameters) are used to mitigate SQL injection risks for query *values*.
- Avoid constructing SQL queries by directly concatenating user-provided strings for *values*. Always use parameterized queries or SQLAlchemy's expression language for values.
- As noted, table/column *identifiers* in the Data Quality service are constructed using f-strings with basic validation. This requires careful handling and trust in the source of these identifiers.

### Cross-Site Scripting (XSS)
- The API primarily returns JSON data. Standard content types for JSON (`application/json`) do not execute scripts in browsers, minimizing direct XSS risks from the API itself.
- If the frontend renders data from the API into HTML, the frontend is responsible for proper escaping/sanitization of that data.

### HTTPS/SSL/TLS
- In a production environment, the FastAPI application should be deployed behind a reverse proxy (e.g., Nginx, Traefik) that handles SSL/TLS termination.
- This ensures that all communication between clients and the API is encrypted (HTTPS). Do not run with Uvicorn directly exposed to the internet without SSL in production.
- Configure your reverse proxy to set appropriate security headers (e.g., `Strict-Transport-Security`).

### Authentication and Authorization
- Currently, the API does not implement authentication or authorization. This is a critical component for production systems and should be implemented based on project requirements (e.g., OAuth2, API Keys, Keycloak, LDAP integration).
- Endpoints that trigger actions (like DQ checks or Airflow sync) or expose potentially sensitive data should be protected.

### Dependency Management
- Keep dependencies up-to-date to patch known vulnerabilities. Regularly audit your dependencies (e.g., using `pip-audit` or GitHub Dependabot). `poetry show --latest` can help identify outdated packages.

---
*This README provides a general guide. Specific deployment and operational details may vary.*
