%sql

CREATE OR REPLACE TABLE gold_bi_asset_monitoring AS
SELECT
    asset_id,
    asset_name,
    asset_type,
    workbook_name,
    project_name,
    owner_name,
    owner_id,
    last_updated,
    last_viewed,
    views_last_30d,
    total_views,
    refresh_status,
    CASE
        WHEN refresh_status = 'failed' THEN 'failing_refresh'
        WHEN views_last_30d = 0 THEN 'unused'
        WHEN views_last_30d IS NULL AND total_views = 0 THEN 'unused'
        WHEN last_updated < current_timestamp() - INTERVAL 90 DAYS THEN 'stale'
        ELSE 'active'
    END AS status,
    extracted_at AS last_synced_at
FROM silver_tableau_assets_enriched;
