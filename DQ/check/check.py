from abc import ABC, abstractmethod
from client.client import BaseDatabaseClient
from dq_result.result import DqResult

class BaseCheck(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def run(self, client: BaseDatabaseClient) -> DqResult:
        pass
