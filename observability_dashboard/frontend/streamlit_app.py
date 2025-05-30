import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta, timezone
import json # For parsing comma-separated strings if needed, and displaying JSON

# --- Configuration and Setup ---
BACKEND_URL = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Observability Dashboard", layout="wide")

# --- Helper Functions for API Calls ---
def fetch_data(endpoint, params=None):
    try:
        url = f"{BACKEND_URL}/{endpoint}"
        st.sidebar.caption(f"Fetching: {url} with params: {params}")
        response = requests.get(url, params=params, timeout=15) # Increased timeout
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        st.error(f"Error: Request timed out connecting to {endpoint}.")
        return None
    except requests.exceptions.ConnectionError:
        st.error(f"Error: Could not connect to the backend at {BACKEND_URL}. Please ensure the backend is running.")
        return None
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP error fetching data from {endpoint}: {e.response.status_code} {e.response.reason}")
        try:
            detail = e.response.json().get("detail", e.response.text)
            st.error(f"Detail: {detail}")
        except requests.exceptions.JSONDecodeError:
            st.error(f"Raw response: {e.response.text[:200]}")
        return None
    except requests.exceptions.JSONDecodeError:
        st.error(f"Error: Could not decode JSON response from {endpoint}.")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred when fetching from {endpoint}: {e}")
        return None

def post_data(endpoint, json_data=None):
    try:
        url = f"{BACKEND_URL}/{endpoint}"
        st.sidebar.caption(f"Posting to: {url}")
        response = requests.post(url, json=json_data, timeout=15)
        response.raise_for_status()
        try:
            return response.json() # Try to parse JSON response first
        except requests.exceptions.JSONDecodeError:
            return response.text # Return text if not JSON (e.g. simple success message)
    except requests.exceptions.Timeout:
        st.error(f"Error: Request timed out connecting to {endpoint}.")
        return None
    except requests.exceptions.ConnectionError:
        st.error(f"Error: Could not connect to the backend at {BACKEND_URL}. Please ensure the backend is running.")
        return None
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP error posting data to {endpoint}: {e.response.status_code} {e.response.reason}")
        try:
            detail = e.response.json().get("detail", e.response.text)
            st.error(f"Detail: {detail}")
        except requests.exceptions.JSONDecodeError:
            st.error(f"Raw response: {e.response.text[:200]}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred when posting to {endpoint}: {e}")
        return None

# --- Sidebar Navigation ---
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    ["Job Overview", "DAG Performance", "Job History", "Data Quality"]
)

# --- Page Implementations ---

if page == "Job Overview":
    st.header("Job Overview")
    
    # Date range selector
    # Common practice for timezone-aware datetimes from date_input is to combine with time_input if needed,
    # or handle timezone conversion appropriately. For simplicity, treating as naive then converting.
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)

    col1, col2 = st.columns(2)
    with col1:
        start_date_query = st.date_input("Start Date", yesterday)
    with col2:
        end_date_query = st.date_input("End Date", today)

    if st.button("Fetch Job Status"):
        if start_date_query and end_date_query:
            # Convert date to datetime start and end of day for query, and make timezone aware
            start_time = datetime(start_date_query.year, start_date_query.month, start_date_query.day, 0, 0, 0, tzinfo=timezone.utc)
            end_time = datetime(end_date_query.year, end_date_query.month, end_date_query.day, 23, 59, 59, tzinfo=timezone.utc)
            
            params = {"start_time": start_time.isoformat(), "end_time": end_time.isoformat()}
            with st.spinner("Fetching job status..."):
                status_data = fetch_data("jobs/status", params=params)
            
            if status_data:
                st.subheader("Job Counts")
                cols = st.columns(4)
                cols[0].metric("Total Jobs", status_data.get("total_jobs", "N/A"))
                cols[1].metric("Successful Jobs", status_data.get("successful_jobs", "N/A"))
                cols[2].metric("Failed Jobs", status_data.get("failed_jobs", "N/A"))
                cols[3].metric("Running Jobs", status_data.get("running_jobs", "N/A"))
            else:
                st.info("No job status data to display for the selected period or an error occurred.")
        else:
            st.warning("Please select both start and end dates.")

elif page == "DAG Performance":
    st.header("DAG Performance")
    
    today = datetime.now(timezone.utc).date()
    seven_days_ago = today - timedelta(days=7)
    col1, col2 = st.columns(2)
    with col1:
        start_date_perf = st.date_input("Start Date", seven_days_ago, key="perf_start")
    with col2:
        end_date_perf = st.date_input("End Date", today, key="perf_end")

    dag_id_perf_filter = st.text_input("Filter by DAG ID (optional):", key="dag_id_perf")

    if st.button("Fetch DAG Performance"):
        if start_date_perf and end_date_perf:
            start_time = datetime(start_date_perf.year, start_date_perf.month, start_date_perf.day, 0, 0, 0, tzinfo=timezone.utc)
            end_time = datetime(end_date_perf.year, end_date_perf.month, end_date_perf.day, 23, 59, 59, tzinfo=timezone.utc)
            
            params = {"start_time": start_time.isoformat(), "end_time": end_time.isoformat()}
            if dag_id_perf_filter:
                params["dag_id"] = dag_id_perf_filter
            
            with st.spinner("Fetching DAG performance data..."):
                performance_data = fetch_data("dags/performance", params=params)

            if performance_data:
                df_perf = pd.DataFrame(performance_data)
                st.subheader("Performance Metrics")
                st.dataframe(df_perf, use_container_width=True)

                if not df_perf.empty:
                    st.subheader("Visualizations")
                    # Bar chart for average duration
                    if 'avg_duration' in df_perf.columns and df_perf['avg_duration'].notna().any():
                        fig_avg = px.bar(df_perf.dropna(subset=['avg_duration']), x="dag_id", y="avg_duration", title="Average DAG Duration (seconds)")
                        st.plotly_chart(fig_avg, use_container_width=True)
                    else:
                        st.info("No average duration data to plot.")
                    
                    # Success Rate
                    if 'success_rate' in df_perf.columns and df_perf['success_rate'].notna().any():
                        fig_sr = px.bar(df_perf.dropna(subset=['success_rate']), x="dag_id", y="success_rate", title="DAG Success Rate (%)", range_y=[0,100])
                        st.plotly_chart(fig_sr, use_container_width=True)
                    else:
                        st.info("No success rate data to plot.")

            else:
                st.info("No DAG performance data to display or an error occurred.")
        else:
            st.warning("Please select both start and end dates for performance data.")


elif page == "Job History":
    st.header("Job History")

    # Filters
    # TODO: Populate DagRunStatus from an endpoint or define manually if stable
    # For now, manual list based on common statuses
    status_options = ["", "success", "failed", "running"] # Add more as needed from your enum
    
    col1, col2, col3 = st.columns([2,1,1])
    with col1:
        dag_id_hist_filter = st.text_input("Filter by DAG ID (contains):", key="dag_id_hist")
    with col2:
        status_hist_filter = st.selectbox("Filter by Status:", options=status_options, key="status_hist")
    
    # Date Range
    today_hist = datetime.now(timezone.utc).date()
    thirty_days_ago_hist = today_hist - timedelta(days=30)
    with col3: # Placeholder for date range or other filters
        st.write("") # Spacing
        
    sub_col1, sub_col2 = st.columns(2)
    with sub_col1:
        start_date_hist = st.date_input("Start Date", thirty_days_ago_hist, key="hist_start")
    with sub_col2:
        end_date_hist = st.date_input("End Date", today_hist, key="hist_end")


    # Pagination
    if 'current_page_history' not in st.session_state:
        st.session_state.current_page_history = 1
    
    page_col1, page_col2, page_col3 = st.columns([1,2,1])
    with page_col1:
        if st.button("⬅️ Previous Page", key="prev_hist"):
            if st.session_state.current_page_history > 1:
                st.session_state.current_page_history -= 1
    with page_col2:
        page_num_input = st.number_input(
            "Page", 
            min_value=1, 
            value=st.session_state.current_page_history, 
            key="page_num_hist_input",
            # on_change=lambda: setattr(st.session_state, 'current_page_history', st.session_state.page_num_hist_input) # Not working as expected
        )
        st.session_state.current_page_history = page_num_input # Direct assignment
    with page_col3:
        if st.button("Next Page ➡️", key="next_hist"):
            st.session_state.current_page_history += 1 # Logic to check max page needed if known

    page_size_hist = st.select_slider("Jobs per page:", options=[10, 20, 50, 100], value=20, key="page_size_hist")

    # Fetch and display
    if st.button("Fetch Job History", key="fetch_hist_btn"):
        params = {
            "page": st.session_state.current_page_history,
            "page_size": page_size_hist
        }
        if dag_id_hist_filter:
            params["dag_id"] = dag_id_hist_filter
        if status_hist_filter:
            params["status"] = status_hist_filter
        
        if start_date_hist and end_date_hist:
            params["start_time"] = datetime(start_date_hist.year, start_date_hist.month, start_date_hist.day, 0,0,0, tzinfo=timezone.utc).isoformat()
            params["end_time"] = datetime(end_date_hist.year, end_date_hist.month, end_date_hist.day, 23,59,59, tzinfo=timezone.utc).isoformat()

        with st.spinner("Fetching job history..."):
            history_data = fetch_data("jobs/history", params=params)
        
        if history_data:
            df_history = pd.DataFrame(history_data)
            st.dataframe(df_history, use_container_width=True)
            if df_history.empty and st.session_state.current_page_history > 1:
                st.info("No more data on this page. Try going to the previous page.")
            elif df_history.empty:
                 st.info("No job history found matching your criteria.")
        else:
            st.info("No job history data to display or an error occurred.")
            if st.session_state.current_page_history > 1 : # If error on higher page, reset to 1
                st.session_state.current_page_history = 1


elif page == "Data Quality":
    st.header("Data Quality Management")

    dq_tabs = st.tabs(["List Application Tables", "Run DQ Checks", "View DQ Metrics"])

    with dq_tabs[0]: # List Application Tables
        st.subheader("Discover Application Database Tables")
        dq_schema_name_list = st.text_input("Schema Name (optional, defaults to DB default):", key="dq_schema_list")
        if st.button("List Tables & Columns", key="list_tables_btn"):
            params = {}
            if dq_schema_name_list:
                params["schema_name"] = dq_schema_name_list
            with st.spinner("Fetching table information..."):
                tables_info = fetch_data("data-quality/list-application-tables", params=params)
            if tables_info is not None: # Check for None explicitly, as empty dict is valid
                if tables_info:
                    st.success("Tables and columns fetched successfully!")
                    st.json(tables_info) # Display as JSON, or iterate for nicer formatting
                    # For nicer display:
                    # for table, columns in tables_info.items():
                    #    with st.expander(table):
                    #        st.write(columns)
                else:
                    st.info("No tables found for the specified schema or an error occurred.")
            # If tables_info is None, fetch_data already showed an error.

    with dq_tabs[1]: # Run DQ Checks
        st.subheader("Run Data Quality Checks")
        with st.form("run_dq_checks_form"):
            dq_table_name = st.text_input("Table Name *", placeholder="e.g., orders")
            dq_schema_name = st.text_input("Schema Name (optional)", placeholder="e.g., public")
            
            dq_cols_null_str = st.text_input("Columns for Null Check (comma-separated)", placeholder="e.g., email,user_id")
            dq_cols_dup_str = st.text_input("Columns for Duplicate Check (comma-separated)", placeholder="e.g., order_id")
            
            dq_run_row_count = st.checkbox("Run Row Count Check", True)
            
            dq_dag_id = st.text_input("Associated DAG ID (optional)")
            dq_task_id = st.text_input("Associated Task ID (optional)")
            
            submitted_run_dq = st.form_submit_button("Run DQ Checks")

        if submitted_run_dq:
            if not dq_table_name:
                st.warning("Table Name is required.")
            else:
                payload = {
                    "table_name": dq_table_name,
                    "schema_name": dq_schema_name if dq_schema_name else None,
                    "columns_for_null_check": [col.strip() for col in dq_cols_null_str.split(',') if col.strip()] if dq_cols_null_str else None,
                    "columns_for_duplicate_check": [col.strip() for col in dq_cols_dup_str.split(',') if col.strip()] if dq_cols_dup_str else None,
                    "run_row_count": dq_run_row_count,
                    "dag_id": dq_dag_id if dq_dag_id else None,
                    "task_id": dq_task_id if dq_task_id else None,
                }
                with st.spinner("Initiating DQ checks..."):
                    response = post_data("data-quality/run-checks", json_data=payload)
                if response:
                    if isinstance(response, dict) and response.get("message"):
                        st.success(response["message"])
                    else:
                        st.info(f"Response from server: {response}")
                # If response is None, post_data already showed an error

    with dq_tabs[2]: # View DQ Metrics
        st.subheader("View Data Quality Metrics")
        
        # Filters for DQ Metrics
        dq_filter_table = st.text_input("Filter by Table Name:", key="dq_metrics_table")
        dq_filter_metric = st.text_input("Filter by Metric Name:", key="dq_metrics_name")
        dq_filter_dag_id = st.text_input("Filter by DAG ID:", key="dq_metrics_dag_id")

        # Pagination for DQ Metrics
        if 'current_page_dq_metrics' not in st.session_state:
            st.session_state.current_page_dq_metrics = 1
        
        dq_page_col1, dq_page_col2, dq_page_col3 = st.columns([1,2,1])
        with dq_page_col1:
            if st.button("⬅️ Previous", key="prev_dq"):
                if st.session_state.current_page_dq_metrics > 1:
                    st.session_state.current_page_dq_metrics -= 1
        with dq_page_col2:
            dq_page_num_input = st.number_input(
                "Page", 
                min_value=1, 
                value=st.session_state.current_page_dq_metrics, 
                key="page_num_dq_input"
            )
            st.session_state.current_page_dq_metrics = dq_page_num_input
        with dq_page_col3:
            if st.button("Next ➡️", key="next_dq"):
                st.session_state.current_page_dq_metrics += 1
        
        dq_page_size = st.select_slider("Metrics per page:", options=[10, 20, 50, 100], value=20, key="dq_page_size")

        if st.button("Fetch DQ Metrics", key="fetch_dq_metrics_btn"):
            params = {
                "page": st.session_state.current_page_dq_metrics,
                "page_size": dq_page_size,
            }
            if dq_filter_table: params["table_name"] = dq_filter_table
            if dq_filter_metric: params["metric_name"] = dq_filter_metric
            if dq_filter_dag_id: params["dag_id"] = dq_filter_dag_id
            
            with st.spinner("Fetching DQ metrics..."):
                dq_metrics_data = fetch_data("data-quality/metrics", params=params)
            
            if dq_metrics_data:
                df_dq_metrics = pd.DataFrame(dq_metrics_data)
                st.dataframe(df_dq_metrics, use_container_width=True)
                if df_dq_metrics.empty and st.session_state.current_page_dq_metrics > 1:
                    st.info("No more DQ metrics on this page. Try going to the previous page.")
                elif df_dq_metrics.empty:
                    st.info("No DQ metrics found matching your criteria.")
            else:
                st.info("No DQ metrics to display or an error occurred.")
                if st.session_state.current_page_dq_metrics > 1:
                    st.session_state.current_page_dq_metrics = 1

st.sidebar.markdown("---")
st.sidebar.info("Observability Dashboard v0.1.0")
if st.sidebar.button("Refresh Page State (Clear Cache)"): # More of a dev tool
    st.experimental_rerun()

# To run this app:
# 1. Ensure backend is running at BACKEND_URL.
# 2. Navigate to observability_dashboard/frontend
# 3. Install dependencies: pip install -r requirements.txt (streamlit, requests, pandas, plotly)
# 4. Run: streamlit run streamlit_app.py
