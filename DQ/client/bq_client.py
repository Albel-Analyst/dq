import pandas_gbq

from pandas import DataFrame
from .client import BaseDatabaseClient
from google.oauth2 import service_account
from google.cloud import bigquery
from typing import Any, List
from io import BytesIO
from dq_result.result import DqResult


class BigQueryClient(BaseDatabaseClient):
    def __init__(self, conn_params: Any) -> None:
        super().__init__(conn_params)
        self.conn = service_account.Credentials.from_service_account_info(
            self.conn_params,
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
            ],
        )
        self.client_bq = bigquery.Client(credentials=self.conn)

    def query_to_df(self, sql: str) -> DataFrame:
        return pandas_gbq.read_gbq(sql, credentials=self.conn, progress_bar_type=None)

    def bq_to_pd_types(self, type_: str) -> str:
        if type_ in {"DATETIME", "DATE"}:
            return "datetime64"
        elif type_ == "FLOAT":
            return "float64"
        elif type_ == "INTEGER":
            return "int64"
        elif type_ == "STRING":
            return "str"
        elif type_ == "TIMESTAMP":
            return "datetime64[s]"

    def make_job_config(
        self, schema: List[bigquery.SchemaField], write_disposition: str
    ) -> bigquery.LoadJobConfig:
        job_config = bigquery.LoadJobConfig()
        job_config.schema = schema
        job_config.write_disposition = write_disposition
        job_config.create_disposition = "CREATE_IF_NEEDED"
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.allow_jagged_rows = True
        job_config.field_delimiter = ","
        job_config.skip_leading_rows = 1
        job_config.schema_update_options = [
            "ALLOW_FIELD_ADDITION",
            "ALLOW_FIELD_RELAXATION",
        ]
        return job_config

    def df_bq_schema_apply(
        self, df: DataFrame, schema: List[bigquery.SchemaField]
    ) -> DataFrame:
        types = {x.name: self.bq_to_pd_types(x.field_type) for x in schema}
        df = df.astype(dtype=types, errors="ignore")
        return df[[x.name for x in schema]]

    def write_df_to_table(
        self,
        df: DataFrame,
        schema: List[bigquery.SchemaField],
        table_id: str,
        write_disposition="WRITE_APPEND",
    ) -> None:
        job_config = self.make_job_config(schema, write_disposition)

        df = self.df_bq_schema_apply(df, schema)
        df = df.replace({"nan": None})

        byte_stream = BytesIO()
        df.to_csv(byte_stream, index=False)
        byte_stream.seek(0)

        job = self.client_bq.load_table_from_file(
            byte_stream, table_id, job_config=job_config, rewind=True
        )
        job.result()
