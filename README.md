# Tableau Metadata Monitoring Pipeline — Databricks Edition

## Repository Contents
- `notebooks/01_tableau_ingestion.py` — Python ingestion notebook logic for loading Tableau metadata into bronze Delta tables
- `notebooks/02_silver_transform.sql` — SQL transformations for silver-layer standardization and enrichment
- `notebooks/03_gold_monitoring.sql` — SQL transformation for the final gold monitoring table
- `images/databricks_workflow_success.png` — screenshot of the successful Databricks workflow run
- `images/gold_bi_asset_monitoring.png` — screenshot of the final gold monitoring table
- `README.md` — project overview, architecture, medallion design, and orchestration details

## Overview
This project is the Databricks-based Version 2 of the Tableau Metadata Monitoring Pipeline.

The goal of this version was to adapt the original local pipeline into a more scalable, platform-aligned design using Databricks. The pipeline ingests Tableau Cloud metadata through the Tableau REST API, lands the raw data into bronze Delta tables, transforms and enriches it in silver, and builds a final gold monitoring table for downstream BI monitoring and governance use cases.

## Architecture
The pipeline follows a medallion-style design:

- **Python ingestion** authenticates to Tableau Cloud and extracts workbook and view metadata through the Tableau REST API.
- **Bronze tables** store the raw landed metadata in Delta format.
- **Silver tables** standardize field names, cast data types, and enrich view metadata with workbook-level context.
- **Gold table** creates the final monitoring-ready BI asset dataset with business-friendly status logic.
- **Databricks Workflow** orchestrates the bronze, silver, and gold steps in sequence.

## Bronze Layer
The bronze layer stores raw Tableau metadata in Delta tables:

- `bronze_tableau_workbooks`
- `bronze_tableau_views`

This layer preserves the landed metadata close to source shape while flattening nested API fields enough to support downstream transformations.

The bronze ingestion logic is implemented in `01_tableau_ingestion.py`.

## Silver Layer
The silver layer standardizes and enriches the bronze metadata. It creates:

- `silver_tableau_workbooks`
- `silver_tableau_views`
- `silver_tableau_assets_enriched`

The purpose of the silver layer is to:
- standardize column names
- cast timestamps into usable data types
- infer or preserve workbook context for views
- enrich views with workbook-level fields such as project and owner
- prepare the data for final business-facing monitoring logic

The silver transformations are implemented in `02_silver_transform.sql`.

## Gold Layer
The gold layer creates the final analytics-ready monitoring table:

- `gold_bi_asset_monitoring`

This table includes:
- `asset_id`
- `asset_name`
- `asset_type`
- `workbook_name`
- `project_name`
- `owner_name`
- `owner_id`
- `last_updated`
- `last_viewed`
- `views_last_30d`
- `total_views`
- `refresh_status`
- `status`
- `last_synced_at`

The `status` field is derived to support BI monitoring use cases such as identifying stale, unused, or failing assets.

The gold transformation is implemented in `03_gold_monitoring.sql`.

## Secret Management
Tableau credentials are stored in a Databricks secret scope rather than being hardcoded in the notebook.

The project uses the secret scope:

- `tableau-metadata-scope`

with keys such as:
- `tableau_server`
- `tableau_site_content_url`
- `tableau_pat_name`
- `tableau_pat_secret`

This keeps sensitive credentials out of notebook code and aligns the project more closely with production-style secret handling.

## Workflow Orchestration
The pipeline is orchestrated in Databricks using a workflow with three dependent tasks:

1. `bronze_ingestion`
2. `silver_transform`
3. `gold_monitoring`

Task dependency flow:

```text
bronze_ingestion -> silver_transform -> gold_monitoring
