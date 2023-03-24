from abc import ABC, abstractmethod
from typing import Any
from dq_result.result import DqResult
from pandas import DataFrame
class BaseDatabaseClient(ABC):
    def __init__(self, conn_params: Any) -> None:
        self.conn_params = conn_params

    @abstractmethod
    def query_to_df(self, sql: str) -> DataFrame:
        pass

    @abstractmethod
    def write_df_to_table(self) -> None:
        pass
