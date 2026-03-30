R.A.W. Data Pipeline Builder



Real-time Analytic Workflows



A modular ETL (Extract, Transform, Load) orchestration tool built with Python and Streamlit. This application is designed for Data Engineers to process raw datasets into analysis-ready, production-grade assets with built-in quality auditing.



🚀 Features



Multi-Source Extraction: Support for CSV, Excel (XLSX), and JSON formats.



Automated Validation: Schema enforcement and data type checking.



Smart Cleaning: Whitespace trimming, standardized casing, and intelligent null handling for numeric fields.



Deduplication Engine: Key-based duplicate removal to ensure data integrity.



Data Enrichment: Automated derivation of Age (from Birthdates) and Fiscal Quarters.



Quality Audit Trail: Real-time logging and performance metrics (latency, row counts, issues flagged).



Export Ready: Download processed datasets directly for production use.



🛠️ Installation



Clone the repository:



git clone https://github.com/wpimedia1/change-mechanism-3.git

cd raw-pipeline-builder





Create a virtual environment:



python -m venv venv

source venv/bin/activate  # On Windows: venv\\Scripts\\activate





Install dependencies:



pip install -r requirements.txt





💻 Usage



Run the Streamlit application using the following command:



streamlit run streamlit\_app.py





Workflow:



Upload: Use the sidebar to upload your raw dataset or generate "Messy Sample Data" to test the pipeline.



Configure: Set your deduplication keys (e.g., Email, ID) and cleaning preferences.



Execute: Click "Run Full Pipeline" to trigger the orchestration.



Audit: Review the Data Quality Audit and Process Logs to ensure the data meets your standards.



Export: Download the finalized CSV for downstream analytics.



📊 Pipeline Logic



The core logic resides in the PipelineEngine class, which follows a standard ETL pattern:



extract(): Loads raw data into a Pandas DataFrame.



validate(): Compares data against a required schema.



clean(): Standardizes string formatting and fills gaps.



deduplicate(): Prunes records based on user-defined primary keys.



transform\_and\_enrich(): Adds business value through derived columns.



finalize(): Generates the quality audit report.

