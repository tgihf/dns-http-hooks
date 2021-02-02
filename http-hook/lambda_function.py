from base64 import b64decode
import json
import gzip
from typing import List

import requests


def lambda_handler(event, context):

    pipedream_url = ""

    # Parse HTTPS event
    event_parsed = {
        "protocol": event["requestContext"]["protocol"],
        "host": event["headers"]["Host"],
        "path": event["path"],
        "method": event["httpMethod"],
        "headers": event["headers"],
        "body": event["body"],
        "query_params": event["queryStringParameters"],
        "request_time": event["requestContext"]["requestTime"],
        "request_time_epoch": event["requestContext"]["requestTimeEpoch"],
    }

    with requests.post(
        pipedream_url,
        json={
            "hook_type": "HTTP",
            "event": event_parsed
        }
    ) as response:
        if response.status_code != 200:
            return {
                "statusCode": 500,
                "body": json.dumps(f"Failed to send data to Pipedream -> URL: {pipedream_url}. Hook type: HTTP. Event: {event}")
                }

    return {
        'statusCode': 200,
        'body': json.dumps('Execution successful!')
    }
