import ee
import time
import json
import requests

from .common import _send_request


def get_value(session, project: str, value):
    """
    """
    url = f'https://earthengine.googleapis.com/v1beta/projects/{project}/value:compute'

    serialized = ee.serializer.encode(value, for_cloud_api=True)

    payload = dict(expression=serialized)

    response = _send_request(session,url,payload)

    if response.status_code != 200:
        raise requests.exceptions.RequestException(f"received the following bad status code: {response.status_code}")


    return response.json()['result']