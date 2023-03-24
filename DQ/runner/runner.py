import pandas as pd
from google.cloud import bigquery

from check.check import BaseCheck
from alert.alert import BaseAlertPlatform
from client.client import BaseDatabaseClient
from dq_result.result import DqResult
from datetime import datetime
from typing import List, Union
import numpy as np


class FailedTestException(Exception):
    pass


class BqRunner:
    def __init__(
        self,
        client: BaseDatabaseClient,
        check: BaseCheck,
        alert_platform: BaseAlertPlatform,
        tested_table: str,
        test_name: str,
        dq_stat_table: str,
        dq_stat_schema: List[bigquery.SchemaField],
        test_description: Union[str, float] = np.NaN,
        msg: str = "",
        data_owner: Union[str, float] = np.NaN,
        allow_alerts: bool = True,
        soft_fail: bool = False,
    ) -> None:
        self.client = client
        self.check = check
        self.alert_platform = alert_platform
        self.tested_table = tested_table
        self.test_name = test_name
        self.dq_stat_table = dq_stat_table
        self.dq_stat_schema = dq_stat_schema
        self.test_description = test_description
        self.msg = msg
        self.data_owner = data_owner
        self.allow_alerts = allow_alerts
        self.soft_fail = soft_fail

    def validate(self, check: BaseCheck) -> DqResult:
        return check.run(self.client)

    def write_result_to_db(
        self,
        result: DqResult,
        write_disposition: str = "WRITE_APPEND",
    ) -> None:

        curr_ts = str(datetime.now())
        data = [
            (
                curr_ts,
                self.tested_table,
                self.test_name,
                self.check.type_of_test,
                self.test_description,
                bool(result),
                self.data_owner,
            )
        ]
        df = pd.DataFrame(
            data,
            columns=[
                "timestamp",
                "tested_table",
                "test_name",
                "test_type",
                "test_description",
                "result",
                "data_owner",
            ],
        )

        self.client.write_df_to_table(
            df,
            self.dq_stat_schema,
            self.dq_stat_table,
            write_disposition=write_disposition,
        )

    def alert_and_raise(self, result: DqResult) -> None:
        if not bool(result):
            if self.allow_alerts:
                self.alert_platform.send_alert(
                    result, self.tested_table, self.test_name, self.msg, self.data_owner
                )
            if not self.soft_fail:
                raise FailedTestException(f"Test {self.test_name} failed.")

    def run(self) -> bool:
        result = self.validate(self.check)
        self.write_result_to_db(result)
        self.alert_and_raise(result)
        return bool(result)
