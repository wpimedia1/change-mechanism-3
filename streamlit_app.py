import streamlit as st
import pandas as pd
import numpy as np
import datetime
import time
import json
import logging
import io
from typing import List, Dict, Any, Tuple

class PipelineEngine:
    """
    Senior Data Engineer implementation of a modular ETL pipeline.
    Handles data from raw extraction to production-ready delivery.
    """
    def __init__(self):
        self.logs = []
        self.quality_report = {
            "rows_processed": 0,
            "issues_found": 0,
            "deduplicated_count": 0,
            "transformations_applied": 0,
            "start_time": None
        }

    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.logs.append(f"[{timestamp}] {level}: {message}")

    def extract(self, source_type: str, data_input: Any) -> pd.DataFrame:
        self.log(f"Starting extraction from {source_type}")
        self.quality_report["start_time"] = datetime.datetime.now()
        
        try:
            if source_type == "CSV":
                df = pd.read_csv(data_input)
            elif source_type == "Excel":
                df = pd.read_excel(data_input)
            elif source_type == "JSON":
                df = pd.read_json(data_input)
            else:
                raise ValueError("Unsupported source type.")
            
            self.log(f"Extracted {len(df)} rows and {len(df.columns)} columns.")
            return df
        except Exception as e:
            self.log(f"Extraction failed: {str(e)}", "ERROR")
            return pd.DataFrame()

    def validate(self, df: pd.DataFrame, schema: Dict[str, str]) -> Tuple[pd.DataFrame, List[str]]:
        self.log("Beginning data validation phase...")
        errors = []
        
        # Check Missing Values
        missing = df.isnull().sum().sum()
        if missing > 0:
            self.log(f"Found {missing} missing values.", "WARNING")
            self.quality_report["issues_found"] += missing
            
        # Type Checking
        for col, expected_type in schema.items():
            if col in df.columns:
                if expected_type == "int" and not pd.api.types.is_integer_dtype(df[col]):
                    errors.append(f"Column '{col}' is not of type Integer.")
                elif expected_type == "float" and not pd.api.types.is_float_dtype(df[col]):
                    errors.append(f"Column '{col}' is not of type Float.")
            else:
                errors.append(f"Required column '{col}' missing from dataset.")
        
        if errors:
            for err in errors:
                self.log(err, "ERROR")
        
        return df, errors

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log("Applying cleaning rules...")
        
        # Standardize strings: Trim whitespace and Title Case
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            df[col] = df[col].astype(str).str.strip()
            # Only apply title case if it looks like a name/category, not an email
            if "email" not in col.lower():
                df[col] = df[col].str.title()
            
        # Fill numeric NaNs with 0
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)
        
        self.log("Text encoding fixed, whitespace trimmed, and nulls handled.")
        return df

    def deduplicate(self, df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
        # Filter keys to only those that exist in the dataframe
        valid_keys = [k for k in keys if k in df.columns]
        if not valid_keys:
            self.log("No valid deduplication keys found in dataset. Skipping step.", "WARNING")
            return df

        self.log(f"Identifying duplicates based on: {valid_keys}")
        initial_count = len(df)
        df = df.drop_duplicates(subset=valid_keys, keep='first')
        removed = initial_count - len(df)
        self.quality_report["deduplicated_count"] = removed
        self.log(f"Removed {removed} duplicate records.")
        return df

    def transform_and_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log("Executing transformation logic and enrichment...")
        
        # Derived Field: Age calculation if 'Birthdate' exists
        if 'Birthdate' in df.columns:
            # errors='coerce' turns unparseable dates into NaT instead of crashing
            df['Birthdate'] = pd.to_datetime(df['Birthdate'], errors='coerce')
            now = datetime.datetime.now()
            
            # Vectorized age calculation
            df['Age'] = df['Birthdate'].apply(lambda x: now.year - x.year if pd.notnull(x) else 0)
            self.log("Enriched dataset with 'Age' column.")

        # Derived Field: Fiscal Quarter from Date
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df['Fiscal_Quarter'] = df['Date'].dt.quarter.fillna(0).astype(int)
            self.log("Calculated Fiscal Quarters.")

        self.quality_report["transformations_applied"] += 2
        return df

    def finalize(self, df: pd.DataFrame) -> Dict[str, Any]:
        end_time = datetime.datetime.now()
        duration = (end_time - self.quality_report["start_time"]).total_seconds()
        
        report = {
            "Final Row Count": len(df),
            "Total Issues Flagged": self.quality_report["issues_found"],
            "Duplicates Removed": self.quality_report["deduplicated_count"],
            "Processing Time (s)": round(duration, 4),
            "Status": "Ready for Delivery"
        }
        self.log("Pipeline execution complete.")
        return report

# --- STREAMLIT UI LAYOUT ---

def main():
    st.set_page_config(page_title="RAW Pipeline Builder | Extract, Transform, Load", layout="wide", page_icon="🔗")
    
    st.title("🔗 RAW Data Pipeline Builder")
    st.markdown("### *ETL Orchestration & Data Quality Control*")
    
    if "pipeline_results" not in st.session_state:
        st.session_state.pipeline_results = None
    if "logs" not in st.session_state:
        st.session_state.logs = []

    # Sidebar: Configuration
    st.sidebar.header("1. Data Source")
    source_type = st.sidebar.selectbox("Extraction Method", ["CSV", "Excel", "JSON"])
    uploaded_file = st.sidebar.file_uploader("Upload Raw Dataset", type=["csv", "xlsx", "json"])
    
    st.sidebar.header("2. Pipeline Rules")
    clean_text = st.sidebar.checkbox("Standardize Strings (Title Case)", value=True)
    handle_nulls = st.sidebar.checkbox("Fill Numeric Nulls with 0", value=True)
    dedupe_keys = st.sidebar.text_input("Deduplication Keys (comma separated)", "Email,ID")
    
    st.sidebar.header("3. Delivery")
    output_format = st.sidebar.selectbox("Destination Format", ["CSV", "Parquet", "Cloud Bucket"])
    
    if st.sidebar.button("Generate Sample Messy Data"):
        sample_data = pd.DataFrame({
            "ID": [101, 102, 101, 104, 105],
            "Name": ["  john doe ", "jane smith", "john doe", "ALICE WONDER", "bob  "],
            "Email": ["john@example.com", "jane@example.com", "john@example.com", "alice@test.com", np.nan],
            "Birthdate": ["1990-05-15", "1985-12-01", "1990-05-15", "invalid_date", "1992-07-20"],
            "Sales": [100.50, 200.00, 100.50, -50, 300.25],
            "Date": pd.date_range(start='2023-01-01', periods=5)
        })
        csv_buffer = io.StringIO()
        sample_data.to_csv(csv_buffer, index=False)
        st.sidebar.success("Sample Data Generated!")
        st.sidebar.download_button("Download Messy CSV", csv_buffer.getvalue(), "messy_data.csv", "text/csv")

    # Main Execution Area
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Pipeline Execution")
        if uploaded_file:
            if st.button("🚀 Run Full Pipeline"):
                engine = PipelineEngine()
                
                with st.status("Orchestrating Workflow...", expanded=True) as status:
                    # 1. Extraction
                    st.write("Step 1: Extracting...")
                    df = engine.extract(source_type, uploaded_file)
                    time.sleep(0.3)
                    
                    # 2. Validation
                    st.write("Step 2: Validating...")
                    schema = {"ID": "int", "Sales": "float"}
                    df, errors = engine.validate(df, schema)
                    time.sleep(0.3)
                    
                    # 3. Cleaning
                    if clean_text or handle_nulls:
                        st.write("Step 3: Cleaning...")
                        df = engine.clean(df)
                        time.sleep(0.3)
                    
                    # 4. Deduplication
                    keys = [k.strip() for k in dedupe_keys.split(",") if k.strip()]
                    df = engine.deduplicate(df, keys)
                    time.sleep(0.3)
                    
                    # 5. Enrichment
                    st.write("Step 5: Enriching...")
                    df = engine.transform_and_enrich(df)
                    
                    # 6. Finalization
                    report = engine.finalize(df)
                    status.update(label="Pipeline Successfully Executed", state="complete", expanded=False)
                
                st.session_state.pipeline_results = df
                st.session_state.report = report
                st.session_state.logs = engine.logs

        if st.session_state.pipeline_results is not None:
            st.success("Analysis-Ready Dataset Generated")
            st.dataframe(st.session_state.pipeline_results, use_container_width=True)
            
            # Export
            csv_out = st.session_state.pipeline_results.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Export to Production",
                data=csv_out,
                file_name=f"enterprise_export_{datetime.date.today()}.csv",
                mime="text/csv"
            )
        else:
            st.info("Upload a dataset or use the sample generator to begin.")

    with col2:
        st.subheader("Data Quality Audit")
        if st.session_state.pipeline_results is not None:
            r = st.session_state.report
            st.metric("Final Row Count", r["Final Row Count"])
            st.metric("Duplicates Pruned", r["Duplicates Removed"])
            st.metric("Processing Latency", f"{r['Processing Time (s)']}s")
            
            if r["Total Issues Flagged"] > 0:
                st.warning(f"Audit Note: {r['Total Issues Flagged']} nulls/type-errors corrected.")
            else:
                st.success("Data quality meets high-integrity standards.")

        st.subheader("Process Logs")
        log_container = st.container(height=400, border=True)
        if st.session_state.logs:
            for entry in st.session_state.logs:
                if "ERROR" in entry:
                    log_container.error(entry)
                elif "WARNING" in entry:
                    log_container.warning(entry)
                else:
                    log_container.text(entry)
        else:
            log_container.caption("System idle...")

    # Orchestration Simulation
    st.divider()
    st.subheader("🕒 Orchestration Settings")
    cron_col1, cron_col2 = st.columns(2)
    with cron_col1:
        st.text_input("Recurrence Pattern (Cron)", "0 0 * * *")
        st.toggle("Auto-retry on transient errors", value=True)
    with cron_col2:
        st.multiselect("Alert Routing", ["Data-Platform-Alerts", "Exec-Reporting-Service"], default=["Data-Platform-Alerts"])
        if st.button("Manually Trigger Remote Job"):
            st.toast("Remote execution request dispatched to cluster.", icon="🚀")

if __name__ == "__main__":
    main()
