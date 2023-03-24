import os
from typing import Dict
from check.checks import CustomSqlCheck
from client.bq_client import BigQueryClient
from runner.runner import BqRunner
from alert.slack_alert import SlackAlertPlatform
from runner.history_table import table_id, schema


def run_custom_sql_test(
    BQ_SERVICE_KEY: Dict[str, str],
    slack_token: str,
    slack_owners: Dict[str, str],
    sql_path: str,
    dataset: str,
    table_name: str,
    test_name: str,
    msg: str,
    data_owner: str,
    allow_alerts: bool = True,
    soft_fail: bool = False,
) -> bool:
    tested_table = f"{dataset}.{table_name}"

    with open(
        os.path.join(
            "/home/ubuntu/airflow_data_dags/self_served_dags/sql_files/", sql_path
        ),
        "r",
    ) as f:
        bool_sql = f.read()
    client = BigQueryClient(BQ_SERVICE_KEY)
    check = CustomSqlCheck(bool_sql)
    alert_platform = SlackAlertPlatform(slack_token, slack_owners)

    runner = BqRunner(
        client=client,
        check=check,
        alert_platform=alert_platform,
        tested_table=tested_table,
        test_name=test_name,
        dq_stat_table=table_id,
        dq_stat_schema=schema,
        msg=msg,
        data_owner=data_owner,
        allow_alerts=allow_alerts,
        soft_fail=soft_fail
    )

    return runner.run()
