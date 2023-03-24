from typing import Dict, Any


class DqResult:
    def __init__(self, result: bool, custom_messages: Dict[str, Any] = None) -> None:
        if custom_messages is None:
            custom_messages = {}
        self.result = result
        self.custom_messages = custom_messages

    def __bool__(self) -> bool:
        return self.result
