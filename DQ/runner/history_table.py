from google.cloud import bigquery
from typing import List

DM_DATASET: str = "DQ_STAGE"
TABLE: str = "dq_history"
table_id: str = f"{DM_DATASET}.{TABLE}"

schema: List[bigquery.SchemaField] = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("tested_table", "STRING"),
    bigquery.SchemaField("test_name", "STRING"),
    bigquery.SchemaField("test_type", "STRING"),
    bigquery.SchemaField("test_description", "STRING"),
    bigquery.SchemaField("result", "BOOLEAN"),
    bigquery.SchemaField("data_owner", "STRING"),
]
