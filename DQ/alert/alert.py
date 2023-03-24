from abc import ABC, abstractmethod
from dq_result.result import DqResult
from numpy import NaN
from typing import Union


class BaseAlertPlatform(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def send_alert(
        self,
        dq_result: DqResult,
        target_table: str,
        test_name: str,
        msg: str = "",
        data_owner: Union[str, float] = NaN,
    ):
        pass
