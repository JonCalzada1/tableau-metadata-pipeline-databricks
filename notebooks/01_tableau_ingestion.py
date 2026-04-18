# Databricks notebook source
# --------------------------------------------------
# Tableau Metadata Ingestion -> Bronze Tables
# --------------------------------------------------

import requests
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

# --------------------------------------------------
# Configuration
# Replace these with Databricks secrets later
# --------------------------------------------------
TABLEAU_SERVER = dbutils.secrets.get(scope='tableau-metadata-scope', key='tableau_server')
TABLEAU_SITE_CONTENT_URL = dbutils.secrets.get(scope='tableau-metadata-scope', key='tableau_site_content_url')
TABLEAU_PAT_NAME = dbutils.secrets.get(scope='tableau-metadata-scope', key='tableau_pat_name')
TABLEAU_PAT_SECRET = dbutils.secrets.get(scope='tableau-metadata-scope', key='tableau_pat_secret')

TABLEAU_API_VERSION = '3.19'
PAGE_SIZE = 100

# --------------------------------------------------
# Helper functions
# --------------------------------------------------
def safe_get(d: Any, key: str, default=None):
    return d.get(key, default) if isinstance(d, dict) else default

def utc_now():
    return datetime.now(timezone.utc)

def infer_workbook_name_from_content_url(content_url: Optional[str]) -> Optional[str]:
    if not content_url:
        return None

    parts = content_url.split('/sheets/')
    if len(parts) == 2:
        return parts[0]

    return None

# --------------------------------------------------
# Tableau API functions
# --------------------------------------------------
def sign_in() -> Dict[str, str]:
    url = f'{TABLEAU_SERVER}/api/{TABLEAU_API_VERSION}/auth/signin'

    payload = {
        'credentials': {
            'personalAccessTokenName': TABLEAU_PAT_NAME,
            'personalAccessTokenSecret': TABLEAU_PAT_SECRET,
            'site': {
                'contentUrl': TABLEAU_SITE_CONTENT_URL
            }
        }
    }

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    credentials = data.get('credentials', {})
    token = credentials.get('token')
    site_id = safe_get(credentials.get('site', {}), 'id')

    if not token or not site_id:
        raise ValueError('Missing Tableau token or site_id.')

    return {'token': token, 'site_id': site_id}

def sign_out(token: str) -> None:
    url = f'{TABLEAU_SERVER}/api/{TABLEAU_API_VERSION}/auth/signout'
    headers = {'X-Tableau-Auth': token}
    requests.post(url, headers=headers, timeout=30)

def tableau_get(token: str, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers = {
        'X-Tableau-Auth': token,
        'Accept': 'application/json'
    }

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def fetch_paginated_items(
    token: str,
    site_id: str,
    endpoint: str,
    root_key: str,
    item_key: str
) -> List[Dict[str, Any]]:
    all_items = []
    page_number = 1

    while True:
        url = f'{TABLEAU_SERVER}/api/{TABLEAU_API_VERSION}/sites/{site_id}/{endpoint}'
        params = {
            'pageSize': PAGE_SIZE,
            'pageNumber': page_number
        }

        data = tableau_get(token, url, params=params)
        root = data.get(root_key, {})
        items = root.get(item_key, [])

        if isinstance(items, dict):
            items = [items]

        pagination = data.get('pagination', {})
        total_available = int(pagination.get('totalAvailable', 0))

        all_items.extend(items)

        if len(all_items) >= total_available or len(items) == 0:
            break

        page_number += 1

    return all_items

def get_workbooks(token: str, site_id: str) -> List[Dict[str, Any]]:
    return fetch_paginated_items(
        token=token,
        site_id=site_id,
        endpoint='workbooks',
        root_key='workbooks',
        item_key='workbook'
    )

def get_views(token: str, site_id: str) -> List[Dict[str, Any]]:
    return fetch_paginated_items(
        token=token,
        site_id=site_id,
        endpoint='views',
        root_key='views',
        item_key='view'
    )

# --------------------------------------------------
# Bronze transformations
# --------------------------------------------------
def transform_bronze_workbooks(workbooks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted_at = utc_now()
    records = []

    for wb in workbooks:
        owner = safe_get(wb, 'owner', {})
        project = safe_get(wb, 'project', {})

        records.append({
            'id': safe_get(wb, 'id'),
            'name': safe_get(wb, 'name'),
            'content_url': safe_get(wb, 'contentUrl'),
            'webpage_url': safe_get(wb, 'webpageUrl'),
            'project_name': safe_get(project, 'name'),
            'owner_id': safe_get(owner, 'id'),
            'owner_name': safe_get(owner, 'name'),
            'updated_at': safe_get(wb, 'updatedAt'),
            'view_count': safe_get(wb, 'viewCount'),
            'extracted_at': extracted_at
        })

    return [r for r in records if r['id']]

def transform_bronze_views(views: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted_at = utc_now()
    records = []

    for view in views:
        owner = safe_get(view, 'owner', {})
        project = safe_get(view, 'project', {})
        workbook = safe_get(view, 'workbook', {})

        content_url = safe_get(view, 'contentUrl')
        nested_workbook_name = safe_get(workbook, 'name')
        inferred_workbook_name = infer_workbook_name_from_content_url(content_url)

        records.append({
            'id': safe_get(view, 'id'),
            'name': safe_get(view, 'name'),
            'content_url': content_url,
            'workbook_name': nested_workbook_name or inferred_workbook_name,
            'project_name': safe_get(project, 'name'),
            'owner_id': safe_get(owner, 'id'),
            'owner_name': safe_get(owner, 'name'),
            'updated_at': safe_get(view, 'updatedAt'),
            'view_count': safe_get(view, 'viewCount'),
            'last_viewed': safe_get(view, 'lastViewedAt'),
            'views_last_30d': safe_get(view, 'viewsLast30d'),
            'extracted_at': extracted_at
        })

    return [r for r in records if r['id']]


workbooks_schema = StructType([
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('content_url', StringType(), True),
    StructField('webpage_url', StringType(), True),
    StructField('project_name', StringType(), True),
    StructField('owner_id', StringType(), True),
    StructField('owner_name', StringType(), True),
    StructField('updated_at', StringType(), True),
    StructField('view_count', IntegerType(), True),
    StructField('extracted_at', TimestampType(), True)
])

views_schema = StructType([
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('content_url', StringType(), True),
    StructField('workbook_name', StringType(), True),
    StructField('project_name', StringType(), True),
    StructField('owner_id', StringType(), True),
    StructField('owner_name', StringType(), True),
    StructField('updated_at', StringType(), True),
    StructField('view_count', IntegerType(), True),
    StructField('last_viewed', StringType(), True),
    StructField('views_last_30d', IntegerType(), True),
    StructField('extracted_at', TimestampType(), True)
])

# --------------------------------------------------
# Main ingestion
# --------------------------------------------------
auth = sign_in()
token = auth['token']
site_id = auth['site_id']

try:
    workbooks = get_workbooks(token, site_id)
    views = get_views(token, site_id)

    bronze_workbooks = transform_bronze_workbooks(workbooks)
    bronze_views = transform_bronze_views(views)

    workbooks_df = spark.createDataFrame(bronze_workbooks, schema=workbooks_schema)
    views_df = spark.createDataFrame(bronze_views, schema=views_schema)

    workbooks_df.write.mode('overwrite').format('delta').saveAsTable('bronze_tableau_workbooks')
    views_df.write.mode('overwrite').format('delta').saveAsTable('bronze_tableau_views')

    print(f'Loaded {len(bronze_workbooks)} workbook records into bronze_tableau_workbooks')
    print(f'Loaded {len(bronze_views)} view records into bronze_tableau_views')

finally:
    sign_out(token)
