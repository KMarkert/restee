import ee
import time
import json
import requests
import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm
from io import BytesIO
from pyproj import CRS
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor

from .common import Domain, _send_request
from .values import get_value


def image_to_xarray( 
    session, 
    project: str, 
    domain: Domain, 
    image: ee.Image, 
    bands: Iterable = None,
    apply_mask: bool = True,
    no_data_value: float = None
):

    if bands is None:
        bands = get_value(session,project,image.bandNames())

    pixels = get_image(session,project,domain,image,bands)

    bandnames = pixels.dtype.names

    if CRS.from_string(domain.crs).is_geographic:
        x_name,y_name = "lon","lat"
        x_long,y_long = "Longitude","Latitude"
        x_units,y_units = "degrees_east","degrees_north"        
    
    else:
        x_name,y_name = "x","y",
        x_long,y_long = "Eastings","Northings"
        x_units,y_units = "meters","meters" # assumes all non-geographic projections have m units...

    coords = {
        x_name: ([x_name],domain.x_coords,
            {"units":x_units,"long_name":x_long}),
        y_name: ([y_name],domain.y_coords,
            {"units":y_units,"long_name":y_long}),
    }

    data_dict = {band:([y_name,x_name],pixels[band]) for band in bandnames}

    ds = xr.Dataset(
        data_dict,
        coords=coords
    )

    if no_data_value is not None:
        ds = ds.where(ds!=no_data_value)

    if apply_mask:
        ds = ds.where(domain.mask == 1)

    return ds

def imgcollection_to_xarray(
    session, 
    project: str, 
    domain: Domain, 
    imagecollection: ee.ImageCollection, 
    bands: Iterable = None,
    max_workers: int = 5,
    verbose: bool = False,
    apply_mask: bool = True,
    no_data_value: float = None
):

    dates = get_value(session,project,imagecollection.aggregate_array("system:time_start"))
    dates = pd.to_datetime(list(map(lambda x: x/1e-6,dates)))

    coll_id = get_value(session,project,imagecollection.get("system:id"))

    n_imgs = get_value(session,project,imagecollection.size())

    if bands is None:
        bands = get_value(session,project,ee.Image(imagecollection.first()).bandNames())

    imgseq = range(n_imgs)
    imglist = imagecollection.toList(n_imgs)

    request_func = lambda x: get_image(session,project,domain,ee.Image(imglist.get(x)),bands=bands)

    if n_imgs < max_workers:
        gen = map(request_func,imgseq)

        if verbose:
            series = tuple(
                tqdm(gen, total=n_imgs,desc=f"{coll_id} progress")
            )
        else:
            series = tuple(gen)

    else:
        with ThreadPoolExecutor(max_workers) as executor:
            gen = executor.map(request_func,imgseq)

            if verbose:
                series = tuple(
                    tqdm(gen, total=n_imgs,desc=f"{coll_id} progress")
                )
            else:
                series = tuple(gen)

    if CRS.from_string(domain.crs).is_geographic:
        x_name,y_name = "lon","lat"
        x_long,y_long = "Longitude","Latitude"
        x_units,y_units = "degrees_east","degrees_north"        
    
    else:
        x_name,y_name = "x","y",
        x_long,y_long = "Eastings","Northings"
        x_units,y_units = "meters","meters" # assumes all non-geographic projections have m units...

    data_dict = {
        'time': {'dims': ('time'), 'data': dates},
        x_name: {'dims': (x_name), 'data': domain.x_coords,
            'attrs':{'long_name': x_long, 'units': x_units}},
        y_name: {'dims': (y_name), 'data': domain.y_coords,
            'attrs':{'long_name': y_long, 'units': y_units}},
    }

    bandnames = series[0].dtype.names
    series_shp = (n_imgs,domain.y_size,domain.x_size)

    for i in range(n_imgs):
        for band in bandnames:
            if i == 0:
                data_dict[band] = {'dims': ('time',y_name, x_name),
                    'data': np.zeros(series_shp)}
            data_dict[band]['data'][i, :, :] = series[i][band][:, :]
    
    ds = xr.Dataset.from_dict(data_dict)

    if no_data_value is not None:
        ds = ds.where(ds!=no_data_value)

    if apply_mask:
        ds = ds.where(domain.mask == 1)

    return ds


def get_image(
    session, 
    project: str, 
    domain: Domain, 
    image: ee.Image, 
    bands: Iterable = None, 
    dataformat: str = "NPY", 
):
    
    if bands is None:
        bands = get_value(session,project,image.bandNames())

    url = f'https://earthengine.googleapis.com/v1beta/projects/{project}/image:computePixels'

    serialized = ee.serializer.encode(image, for_cloud_api=True)

    payload = dict(
        expression = serialized,
        fileFormat = dataformat,
        bandIds = bands,
        grid = domain.pixelgrid
    )

    response = _send_request(session,url,payload)

    if response.status_code != 200:
        raise requests.exceptions.RequestException(f"received the following bad status code: {response.status_code}")

    if dataformat == "NPY":
        result = np.load(BytesIO(response.content))
    elif dataformat == "GEO_TIFF":
        raise NotImplementedError()
    elif dataformat == "TF_RECORD_IMAGE":
        raise NotImplementedError()
    else:
        raise AttributeError(f"select dataformat {dataformat} is not implemented.Options are 'NPY','GEO_TIFF', or 'TF_RECORD_IMAGE'")

    return result
