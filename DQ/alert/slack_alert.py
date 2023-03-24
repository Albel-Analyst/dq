import requests
import json

from .alert import BaseAlertPlatform
from typing import Dict, Union
from numpy import NaN
from dq_result.result import DqResult


class SlackAlertPlatform(BaseAlertPlatform):
    def __init__(
        self, webhook_url, slack_user_id_map: Union[Dict[str, str], None] = None
    ) -> None:
        self.webhook_url = f"https://hooks.slack.com/services/{webhook_url}"
        self.slack_user_id_map = slack_user_id_map

    def map_slack_user_id(
        self, data_owner: str, slack_user_id_map: Union[Dict[str, str], None] = None
    ) -> str:
        if slack_user_id_map is None:
            return "no mapping provided"
        return slack_user_id_map.get(data_owner, f"no mapping found for {data_owner}")

    def send_alert(
        self,
        dq_result: DqResult,
        target_table: str,
        test_name: str,
        msg: str = "",
        data_owner: Union[str, float] = NaN,
    ) -> requests.Response:            
        data_owner = self.map_slack_user_id(data_owner, self.slack_user_id_map)
        text = f"Fail on *{test_name}* test for *{target_table}* table. {msg}"
        text += "\n\n"
        text += f"<@{data_owner}>"
        if dq_result.custom_messages:
            messages = "\n".join(
                [f"*{k}*: {v}" for k, v in dq_result.custom_messages.items()]
            )
            text += "\n\n"
            text += messages
        payload = {
            "channel": "#dq_alerts",
            "username": "BigQuery DQ Bot",
            "icon_emoji": ":red_circle:",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": text,
                    },
                },
                {"type": "divider"},
            ],
        }
        return requests.post(self.webhook_url, json.dumps(payload))
