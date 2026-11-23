# Global FX & Macroeconomic Analytics — End-to-End Microsoft Fabric Medallion Pipeline

An end-to-end modern data engineering and business intelligence solution built entirely in **Microsoft Fabric**. This project automates the ingestion, transformation, semantic modeling, and executive visualization of global foreign exchange (FX) rates and macroeconomic GDP indicators using a **Medallion Architecture** and **Direct Lake** reporting.

---

## Architecture Overview

```text
+-----------------------------------------------------------------------------------+

|                 pl_finance_medallion_orchestrator (Data Pipeline)                 |
|             Automates end-to-end execution with runtime parameters                |
+-----------------------------------------+-----------------------------------------+
                                          |
                    +---------------------+---------------------+
                    v                                           v
+---------------------------------------+   +---------------------------------------+

|      nb_01_ingest_finance_apis        |   |      nb_02_transform_medallion        |
|          (PySpark Notebook)           |   |          (PySpark Notebook)           |
|  Pulls Frankfurter FX & World Bank    |   |  Cleanses, deduplicates & models      |
|  GDP APIs into OneLake Bronze files   |   |  Silver & Gold Star Schema Delta      |
+-------------------+-------------------+   +-------------------+-------------------+

                    |                                           |
                    +---------------------+---------------------+
                                          v
+-----------------------------------------------------------------------------------+

|                        lh_finance_fx_macro (OneLake Lakehouse)                    |
|              Bronze (Raw JSON/Parquet)  -->  Silver  -->  Gold (Star Schema)      |
+-----------------------------------------+-----------------------------------------+
                                          |  Direct Lake (Zero-Copy)
                                          v
+-----------------------------------------------------------------------------------+

|                   sm_finance_fx_gold (Direct Lake Semantic Model)                 |
|        Custom DAX KPIs: Latest Rate, 45-Day High/Low, 45-Day % Change             |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+

|                     rpt_fx_macro_executive (Power BI Report)                      |
|     Interactive Currency Slicers, KPI Cards, 45-Day Trend & GDP Comparison        |
+-----------------------------------------------------------------------------------+
```

---

## Project Components & Artifacts

| Artifact Name | Fabric Item Type | Role & Responsibility |
| :--- | :--- | :--- |
| **`pl_finance_medallion_orchestrator`** | Data Pipeline | Automates end-to-end execution by passing runtime parameters (e.g., `days_back`) and chaining ingestion and transformation notebooks sequentially. |
| **`nb_01_ingest_finance_apis`** | PySpark Notebook | Extracts historical and daily FX rates (USD base across AUD, CAD, CHF, EUR, GBP, INR, JPY) from the Frankfurter API and GDP metrics from the World Bank API into OneLake Bronze storage. |
| **`nb_02_transform_medallion`** | PySpark Notebook | Standardizes schemas, handles weekend/holiday rate forward-filling, deduplicates records, and writes curated **Silver** and star-schema **Gold** Delta tables. |
| **`lh_finance_fx_macro`** | Lakehouse | Unified OneLake storage layer housing raw API landing zones and ACID-compliant Delta tables for analytics and SQL endpoint querying. |
| **`sm_finance_fx_gold`** | Semantic Model | **Direct Lake** semantic model connected directly to Gold Delta tables (eliminating import refresh latency), enriched with custom DAX KPIs (Latest Rate, 45-Day High/Low, 45-Day % Change). |
| **`rpt_fx_macro_executive`** | Power BI Report | Interactive executive dashboard featuring dynamic currency slicer buttons, KPI summary cards, a 45-day FX trend line, and cross-country GDP comparisons with data labels. |

---

## Key Technical Highlights

* **Unified SaaS Medallion Architecture:** Implemented Bronze (raw ingestion), Silver (cleansed/conformed), and Gold (analytics-ready star schema) layers inside a single OneLake Lakehouse.
* **Zero-Copy Direct Lake Modeling:** Leveraged Microsoft Fabric's Direct Lake storage mode to query Parquet/Delta files directly from OneLake with Import-level speed and no scheduled data duplication.
* **Parameterized & Idempotent Pipelines:** Designed `pl_finance_medallion_orchestrator` with parameterized lookback windows (`days_back`) and safe Delta merge/overwrite patterns to prevent duplicate records on re-runs.
* **Source-Controlled CI/CD (`TMDL` & `PBIR`):** Integrated directly with GitHub via Microsoft Fabric Git Integration, storing semantic models in Tabular Model Definition Language (`.tmdl`) and reports in Power BI Enhanced Report Format (`.pbir`) for readable version control.

---

## Dashboard Preview

<img width="1464" height="643" alt="image" src="https://github.com/user-attachments/assets/fe339b89-a057-44ee-958f-d0d770c628d7" />


---

## How to Deploy in Your Own Fabric Workspace

1. **Fork or Clone this Repository** to your GitHub account.
2. **Create a Fabric Workspace** backed by a Fabric Capacity (`F-SKU` or `Trial`).
3. **Connect Git Integration:**
   * Open **Workspace settings → Git integration → GitHub**.
   * Connect to your repository on the `main` branch and click **Connect and sync**.
4. **Configure Lakehouse Binding:**
   * Open `nb_01_ingest_finance_apis` and `nb_02_transform_medallion` and verify `lh_finance_fx_macro` is attached as the default Lakehouse.
5. **Run the Orchestrator:**
   * Open `pl_finance_medallion_orchestrator` and click **Run** to populate Bronze, Silver, and Gold layers end-to-end.

---

## Tech Stack
* **Platform:** Microsoft Fabric (OneLake, Data Engineering, Data Factory, Power BI)
* **Languages:** PySpark, SQL, DAX, M, TMDL, JSON
* **Data Sources:** Frankfurter Foreign Exchange API, World Bank Open Data API
* **Version Control:** GitHub (Fabric Git Integration)
