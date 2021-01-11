import ee
import json
import geojson
import requests
import geopandas as gpd

from .common import _send_request


def fccollection_to_geopandas(session,project: str,featurecollection: ee.FeatureCollection):
    """
    """
    table = get_table(session,project,featurecollection)

    gdf = gpd.read_file(geojson.dumps(table))

    return gdf

def feature_to_geopandas(session,project: str,feature: ee.Feature):
    """
    """
    fc = ee.FeatureCollection([feature])

    return fccollection_to_geopandas(session, project, fc)

def get_table(session, project: str, featurecollection: ee.FeatureCollection):
    """
    """
    url = f'https://earthengine.googleapis.com/v1beta/projects/{project}/table:computeFeatures'

    serialized = ee.serializer.encode(featurecollection, for_cloud_api=True)

    payload = dict(expression=serialized)

    response = _send_request(session,url,payload)

    if response.status_code != 200:
        raise requests.exceptions.RequestException(f"received the following bad status code: {response.status_code}")

    table = geojson.loads(response.content)

    return table