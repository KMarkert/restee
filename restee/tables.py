import ee
import json
import requests
from io import StringIO
from pathlib import Path
import pandas as pd
import geopandas as gpd

from restee.core import EESession


def features_to_file(
    session: EESession, features: ee.FeatureCollection, outfile: str, driver: str = "GeoJSON"
):
    """Wrapper fuction to save requested ee.Feature or ee.FeatureCollection in a vector format

    args:
        session (EESession): restee session autheticated to make requests
        features (ee.Feature | ee.FeatureCollection): ee.Feature or ee.FeatureCollections save as file
        outfile (str): path to save features
        driver (str): valid vector driver name to save file, see `import fiona; fiona.supported_drivers` 
            for full list of supported drivers . default = "GeoJSON"

    example:
        >>> img = (
                ee.ImageCollection('MODIS/006/MOD13Q1')
                .select("NDVI")
                .first()
            )
        >>> states = ee.FeatureCollection('TIGER/2018/States')
        >>> features = image.reduceRegions(
                collection=maine,
                reducer=ee.Reducer.mean().setOutputs(["NDVI"]),
                scale=image.projection().nominalScale()
            )
        >>> restee.features_to_file(session,features,"state_ndvi.geojson")
    """
    gdf = features_to_geopandas(session, features)

    gdf.to_file(outfile,driver=driver)

    return


def features_to_geodf(session: EESession, features: ee.FeatureCollection):
    """Fuction to request ee.Feature or ee.FeatureCollection as a geopandas GeoDataFrame

    args:
        session (EESession): restee session autheticated to make requests
        features (ee.Feature | ee.FeatureCollection): ee.Feature or ee.FeatureCollections to
            request as a GeoDataFrame

    returns:
        geopandas.GeoDataFrame: ee.FeatureCollection as GeoDataFrame

    example:
        >>> img = (
                ee.ImageCollection('MODIS/006/MOD13Q1')
                .select("NDVI")
                .first()
            )
        >>> states = ee.FeatureCollection('TIGER/2018/States')
        >>> features = image.reduceRegions(
                collection=maine,
                reducer=ee.Reducer.mean().setOutputs(["NDVI"]),
                scale=image.projection().nominalScale()
            )
        >>> gdf = restee.features_to_geopandas(session,features)
    """
    if isinstance(features, ee.Feature):
        features = ee.FeatureCollection([features])

    table = _get_table(session, features)

    return gpd.read_file(StringIO(table.decode()))


def features_to_df(session: EESession, features: ee.FeatureCollection):
    """Fuction to request ee.Feature or ee.FeatureCollection without coordinates as a pandas DataFrame

    args:
        session (EESession): restee session autheticated to make requests
        features (ee.Feature | ee.FeatureCollection): ee.Feature or ee.FeatureCollections to
            request as a DataFrame

    returns:
        pandas.DataFrame: ee.FeatureCollection as DataFrame

    example:
        >>> ndvi = (
                ee.ImageCollection('MODIS/006/MOD13Q1')
                .select("NDVI")
                .first()
            )
        >>> temp = ee.ImageCollection('OREGONSTATE/PRISM/AN81m')
                  .filter(ee.Filter.date('2018-07-01', '2018-07-31'));

        >>> states = ee.FeatureCollection('TIGER/2018/States')
        >>> features = image.reduceRegions(
                collection=maine,
                reducer=ee.Reducer.mean().setOutputs(["NDVI"]),
                scale=image.projection().nominalScale()
            )
        >>> gdf = restee.features_to_geopandas(session,features)
    """
    if isinstance(features, ee.Feature):
        features = ee.FeatureCollection([features])

    table = _get_table(session, features)

    return pd.read_file(StringIO(table.decode()))



def _get_table(session: EESession, featurecollection: ee.FeatureCollection):
    """Base fuction to request ee.Feature or ee.FeatureCollection

    args:
        session (EESession): restee session autheticated to make requests
        featurecollection (ee.FeatureCollection): ee.FeatureCollections to request data from

    returns:
        bytes: raw byte data of table in geojson format requested
    """
    project = session.cloud_project
    url = f"https://earthengine.googleapis.com/v1beta/projects/{project}/table:computeFeatures"

    serialized = ee.serializer.encode(featurecollection, for_cloud_api=True)

    payload = dict(expression=serialized)

    response = session.send_request(url, payload)

    if response.status_code != 200:
        raise requests.exceptions.RequestException(
            f"received the following bad status code: {response.status_code}\nServer message: {response.json()['error']['message']}"
        )

    return response.content
