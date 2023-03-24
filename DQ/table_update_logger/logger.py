from client.bq_client import BigQueryClient, BaseDatabaseClient

class TableUpdateLogger():
    def __init__(self, client: BaseDatabaseClient) -> None:
        self.client = client

    def write_result_to_db(
        self,
        update_ts: str,
        write_disposition: str = "WRITE_APPEND",
    ) -> None:

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