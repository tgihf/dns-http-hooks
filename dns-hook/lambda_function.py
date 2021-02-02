from base64 import b64decode
import json
import gzip
from typing import List

import requests


def lambda_handler(event, context):

    pipedream_url = ""

    # Decode and decompress event
    event: bytes = b64decode(event["awslogs"]["data"])
    event: bytes = gzip.decompress(event)
    event: str = event.decode("utf-8")
    event: dict = json.loads(event)

    # Parse each log event
    log_events_parsed: List[dict] = []
    log_events_raw: List[dict] = event["logEvents"]
    for event in log_events_raw:
        message_elements: List[str] = event["message"].split(" ")
        log_events_parsed.append({
            "id": event["id"],
            "timestamp": event["timestamp"],
            "log_stream": message_elements[2],
            "domain": message_elements[3],
            "record_type": message_elements[4],
            "is_error": message_elements[5] != "NOERROR",
            "protocol": message_elements[6],
            "request_ip": message_elements[8]
        })
    for event in log_events_parsed:
        with requests.post(
            pipedream_url,
            json={
                "hook_type": "DNS",
                "events": log_events_parsed
            }
        ) as response:
            if response.status_code != 200:
                return {
                    "statusCode": 500,
                    "body": json.dumps(f"Failed to send data to Pipedream -> URL: {pipedream_url}. Hook type: DNS. Events: {log_events_parsed}")
                }

    return {
        'statusCode': 200,
        'body': json.dumps('Execution successful!')
    }
