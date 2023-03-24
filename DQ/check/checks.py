from .check import BaseCheck
from client.bq_client import BaseDatabaseClient
from dq_result.result import DqResult
from typing import List


class CustomSqlCheck(BaseCheck):
    def __init__(self, sql) -> None:
        self.sql = sql
        self.type_of_test = "custom_bool_sql"

    def bool_query(self, sql: str, client: BaseDatabaseClient) -> DqResult:
        result = client.query_to_df(sql)
        if len(result) != 1:
            raise ValueError(f"Test returned {len(result)} rows, expected 1")
        if "test_result" not in result.columns:
            raise ValueError(
                f"Test returned ({result.columns}) columns, 'test_result' must be present "
            )
        bool_result = bool(result["test_result"][0])
        if result.shape[1] > 1:
            custom_messages = {
                x: y
                for x, y in result.to_dict(orient="records")[0].items()
                if x != "test_result"
            }
        else:
            custom_messages = {}
        result = DqResult(bool_result, custom_messages)
        return result

    def run(self, client: BaseDatabaseClient) -> DqResult:
        return self.bool_query(self.sql, client)


class AdAccountsGsCheck(BaseCheck):
    def __init__(self, ad_accounts_list: List[str], gs_sheet_name: str) -> None:
        self.ad_accounts_list = set(ad_accounts_list)
        self.gs_sheet_name = gs_sheet_name
        self.type_of_test = "ad_accounts_check"
        self.DEFAULT_ID_COLUMN = "id"
        self.DEFAULT_BQ_SCHEMA = "DQ_STAGE"

    def ad_accounts_check(self, client: BaseDatabaseClient) -> DqResult:
        sql = f"SELECT {self.DEFAULT_ID_COLUMN} FROM {self.DEFAULT_BQ_SCHEMA}.{self.gs_sheet_name} WHERE {self.DEFAULT_ID_COLUMN} IS NOT NULL"
        result = client.query_to_df(sql)
        result = result[~result["id"].isna()]
        gs_ids = set(result["id"])

        custom_messages = {}
        if len(gs_ids) != len(self.ad_accounts_list):
            custom_messages["len_check"] = "Not equal length of accounts"

        if gs_ids.difference(self.ad_accounts_list):
            custom_messages["ACCOUNTS IN GS BUT NOT IN AIRFLOW"] = gs_ids.difference(
                self.ad_accounts_list
            )

        if len(self.ad_accounts_list.difference(gs_ids)) > 0:
            custom_messages[
                "ACCOUNTS IN AIRFLOW BUT NOT IN GS"
            ] = self.ad_accounts_list.difference(gs_ids)

        if custom_messages:
            bool_result = False
        else:
            bool_result = True
            custom_messages = {}
        result = DqResult(bool_result, custom_messages)
        return result

    def run(self, client: BaseDatabaseClient) -> DqResult:
        return self.ad_accounts_check(client)
