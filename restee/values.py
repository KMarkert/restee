import ee
import time
import json
import requests

from restee.core import EESession

def get_value(session: EESession, value):
    """General EE REST API wrapper to request any ee.ComputedObject value
    
    args:
        session (EESession): restee session autheticated to make requests
        value (Any): any ee.ComputedObject to request the value for

    returns:
        Any: Python evaluated equivalent of ee object

    example:
        >>> img = ee.Image("NASA/NASADEM_HGT/001")
        >>> ee_bnames = img.bandNames()
        >>> band_names = restee.get_value(session, ee_bnames)
        >>> print(band_names)
            ['elevation', 'num', 'swb']
    """
    project = session.cloud_project
    url = f'https://earthengine.googleapis.com/v1beta/projects/{project}/value:compute'

    serialized = ee.serializer.encode(value, for_cloud_api=True)

    payload = dict(expression=serialized)

    response = session.send_request(url, payload)

    if response.status_code != 200:
        raise requests.exceptions.RequestException(
            f"received the following bad status code: {response.status_code}\nServer message: {response.json()['error']['message']}")

    return response.json()['result']
