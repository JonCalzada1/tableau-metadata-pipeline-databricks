%sql

CREATE OR REPLACE TABLE silver_tableau_workbooks AS
SELECT
    id AS asset_id,
    name AS workbook_name,
    content_url,
    webpage_url,
    project_name,
    owner_id,
    owner_name,
    CAST(updated_at AS TIMESTAMP) AS last_updated,
    view_count AS total_views,
    extracted_at
FROM bronze_tableau_workbooks;

CREATE OR REPLACE TABLE silver_tableau_views AS
SELECT
    id AS asset_id,
    name AS asset_name,
    content_url,
    workbook_name,
    project_name,
    owner_id,
    owner_name,
    CAST(updated_at AS TIMESTAMP) AS last_updated,
    CAST(last_viewed AS TIMESTAMP) AS last_viewed,
    views_last_30d,
    view_count AS total_views,
    extracted_at
FROM bronze_tableau_views;

CREATE OR REPLACE TABLE silver_tableau_assets_enriched AS
SELECT
    v.asset_id,
    v.asset_name,
    'view' AS asset_type,
    COALESCE(v.workbook_name, w.workbook_name) AS workbook_name,
    COALESCE(v.project_name, w.project_name) AS project_name,
    COALESCE(v.owner_name, w.owner_name) AS owner_name,
    COALESCE(v.owner_id, w.owner_id) AS owner_id,
    v.last_updated,
    v.last_viewed,
    v.views_last_30d,
    v.total_views,
    'unknown' AS refresh_status,
    v.content_url AS web_url,
    v.extracted_at
FROM silver_tableau_views v
LEFT JOIN silver_tableau_workbooks w
  ON LOWER(REPLACE(v.workbook_name, ' ', '')) = LOWER(REPLACE(w.workbook_name, ' ', ''));
