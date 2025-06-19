# DE Assessment PySpark Project

## 📌 Overview
This project processes SCADA sensor data using PySpark. It:
- Loads DB credentials from JSON
- Fetches tag metadata and historical sensor data from PostgreSQL
- Filters by tag path patterns and recent timestamps
- Transforms data (cleaning, timestamping)
- Appends and writes updated records to S3 in Parquet format
