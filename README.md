# Hospital Length of Stay (LOS) Prediction Pipeline
> **A Medallion Architecture implementation on Azure Databricks for clinical data processing and binary classification.**

---

## 1. Project Overview
This project demonstrates an end-to-end ETL and machine learning workflow designed to predict hospital patient length of stay

* **Target Label**: Binary classification where $y = 1$ if $Stay > 10$ days, and $y = 0$ otherwise
* **Performance**: The Random Forest model achieved a Test Area Under ROC (AUC) of approximately **0.9557**

---

## 2. System Architecture
The solution utilizes the Azure ecosystem to automate data movement and model training

* **Orchestration**: Azure Data Factory (ADF) handles HTTP ingestion, unzipping raw files, and triggering the Databricks Job
* **Transformation & ML**: Azure Databricks processes the data through Bronze, Silver, and Gold layers
* **Serving**: Model binaries and prediction results are exported to an Azure SQL Database for downstream application access

---

## 3. Data Engineering (Medallion Architecture)

### 🥉 Bronze Layer (Ingestion)
* Ingests raw medical encounters and condition records in Parquet/CSV format
* Maintains the original state of the source data for auditability.

### 🥈 Silver Layer (Cleaning & Integration)
* **Identity Management**: Generates surrogate keys using `sha2` (256-bit) to uniquely identify patients, providers, and encounters
* **Standardization**: Renames columns to snake_case and casts technical fields (e.g., `VALUE` to `double`) for consistency
* **History Tracking**: Implements **SCD Type 2** for patient data, using `effective_start_date`, `effective_end_date`, and `is_current` flags to track changes over time
* **Relational Joins**: Integrates disparate tables (Encounters, Patients, Providers) into a unified structure

### 🥇 Gold Layer (Feature Engineering)
* **Business Logic**: Calculates `patient_age` at the time of encounter and `observation_count` (total clinical observations per patient)
* **Data Integrity**: Uses `QUALIFY` with `row_number()` to select only the most recent medical record for each encounter
* **Class Balancing**: Addresses the imbalance of short-stay records by performing **random downsampling** (using `rand() < 0.10` for the majority class)

---

## 4. Machine Learning & Governance

### Random Forest Classifier
The model is built using a Spark ML Pipeline for consistency across training and inference
1. **StringIndexer**: Encodes categorical features like `provider_speciality`
2. **VectorAssembler**: Consolidates numerical and encoded features into a single input vector
3. **RandomForest**: Selected for its robustness with categorical healthcare data

### MLOps with MLflow
* **Experiment Tracking**: Automatically logs parameters (e.g., `numTrees`, `maxDepth`) and metrics (AUC)
* **Schema Enforcement**: Implements `infer_signature` to ensure the model only accepts valid data types during inference

---

## 5. Deployment & Integration
The final outputs are written to an **Azure SQL Database** (`dbo.SavedMLModels`)
* **Model Portability**: The model is saved as a Binary Large Object (BLOB), allowing it to be loaded by external applications without a persistent Databricks connection
* **Prediction Logs**: Historical prediction results are appended to dedicated SQL tables for auditing and reporting

---

## 🛠️ Tech Stack
* **Azure Services**: Databricks, Data Factory, ADLS Gen2, SQL Database
* **Libraries**: PySpark SQL, PySpark ML, MLflow
* **Architecture**: Medallion (Bronze, Silver, Gold)
