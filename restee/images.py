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
from pathlib import Path
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor

from restee.core import Domain, EESession
from restee.values import get_value


def img_to_xarray(
    session: EESession,
    domain: Domain,
    image: ee.Image,
    bands: Iterable = None,
    apply_mask: bool = True,
    no_data_value: float = None,
):
    """Function to request ee.Image as a xarray Dataset. This function
    wraps img_to_ndarray.

    args:
        session (EESession): Earth Engine cloud session used to manage REST API requests
        domain (Domain): Domain object defining the spatial region to request image
        image (ee.Image): computed ee.Image object to request
        bands (Iterable[str]): list or tuple or band names to request from image, if None then
            all bands will be requested. default = None
        apply_mask (bool): mask pixels based on domain mask. default = True
        no_data_value (float): no data value to mask in returned dataset, typically 0. if None, 
            then no data will be masked by value. default = None

    returns:
        xarray.Dataset: dataset with geocoordinates and each band as a variable

    example:
        >>> img = (
                ee.ImageCollection('MODIS/006/MOD13Q1')
                .first()
            )
        >>> states = ee.FeatureCollection('TIGER/2018/States')
        >>> maine = states.filter(ee.Filter.eq('NAME', 'Maine'))
        >>> domain = restee.Domain.from_ee_geometry(session,maine,0.01)
        >>> ds_ndvi = ree.img_to_xarray(session,domain,img,no_data_value=0)
    """

    if bands is None:
        bands = get_value(session, image.bandNames())

    pixels = img_to_ndarray(session, domain, image, bands=bands)

    bandnames = pixels.dtype.names

    if CRS.from_string(domain.crs).is_geographic:
        x_name, y_name = "lon", "lat"
        x_long, y_long = "Longitude", "Latitude"
        x_units, y_units = "degrees_east", "degrees_north"

    else:
        x_name, y_name = (
            "x",
            "y",
        )
        x_long, y_long = "Eastings", "Northings"
        # assumes all non-geographic projections have m units...
        x_units, y_units = "meters", "meters"

    # CF conventions are coordinates for center pixels
    # assign domain coordinates and shift to center
    coords = {
        x_name: (
            [x_name],
            domain.x_coords + (domain.resolution / 2),
            {"units": x_units, "long_name": x_long},
        ),
        y_name: (
            [y_name],
            domain.y_coords - (domain.resolution / 2),
            {"units": y_units, "long_name": y_long},
        ),
    }

    data_dict = {band: ([y_name, x_name], pixels[band]) for band in bandnames}

    ds = xr.Dataset(data_dict, coords=coords)

    if no_data_value is not None:
        ds = ds.where(ds != no_data_value)

    if apply_mask:
        ds = ds.where(domain.mask == 1)

    return ds


def imgcollection_to_xarray(
    session,
    domain: Domain,
    imagecollection: ee.ImageCollection,
    bands: Iterable = None,
    max_workers: int = 5,
    verbose: bool = False,
    apply_mask: bool = True,
    no_data_value: float = None,
):
    """Function to request ee.ImageCollection as a xarray Dataset. This function assumes 
    the image collection is distinguished by time (i.e. 'system:time_start' property).
    Sends multiple concurrent requests to speed up data transfer. This function wraps 
    img_to_ndarray.

    args:
        session (EESession): Earth Engine cloud session used to manage REST API requests
        domain (Domain): Domain object defining the spatial region to request image
        imagecollection (ee.ImageCollection): computed ee.ImageCollection object to request, 
            images must have `system:time_start` property
        bands (Iterable[str]): list or tuple or band names to request from image, if None then
            all bands will be requested. default = None
        max_workers (int): number of concurrent requests to send. default = 5,
        verbose (bool): flag to determine if a request progress bar should be shown. default = False
        apply_mask (bool): mask pixels based on domain mask. default = True
        no_data_value (float): no data value to mask in returned dataset, typically 0. if None, 
            then no data will be masked by value. default = None

    returns:
        xarray.Dataset: dataset with multiple images along time dimesions, each band is a variable

    example:
        >>> ic = (
                ee.ImageCollection('MODIS/006/MOD13Q1')
                .limit(10,"system:time_start")
            )
        >>> states = ee.FeatureCollection('TIGER/2018/States')
        >>> maine = states.filter(ee.Filter.eq('NAME', 'Maine'))
        >>> domain = restee.Domain.from_ee_geometry(session,maine,0.01)
        >>> ds_ndvi = ree.imgcollection_to_xarray(session,domain,img,no_data_value=0,verbose=True)
    """

    #TODO: write functionality to allow the definition of ImageCollections by other properties than time

    dates = get_value(session, imagecollection.aggregate_array("system:time_start"))
    dates = pd.to_datetime(list(map(lambda x: x / 1e-6, dates)))

    coll_id = get_value(session, imagecollection.get("system:id"))

    n_imgs = get_value(session, imagecollection.size())

    if bands is None:
        bands = get_value(session, ee.Image(imagecollection.first()).bandNames())

    imgseq = range(n_imgs)
    imglist = imagecollection.toList(n_imgs)

    def request_func(x):
        return img_to_ndarray(session, domain, ee.Image(imglist.get(x)), bands=bands)

    if n_imgs < max_workers:
        gen = map(request_func, imgseq)

        if verbose:
            series = tuple(tqdm(gen, total=n_imgs, desc=f"{coll_id} progress"))
        else:
            series = tuple(gen)

    else:
        with ThreadPoolExecutor(max_workers) as executor:
            gen = executor.map(request_func, imgseq)

            if verbose:
                series = tuple(tqdm(gen, total=n_imgs, desc=f"{coll_id} progress"))
            else:
                series = tuple(gen)

    if CRS.from_string(domain.crs).is_geographic:
        x_name, y_name = "lon", "lat"
        x_long, y_long = "Longitude", "Latitude"
        x_units, y_units = "degrees_east", "degrees_north"

    else:
        x_name, y_name = (
            "x",
            "y",
        )
        x_long, y_long = "Eastings", "Northings"
        # assumes all non-geographic projections have m units...
        x_units, y_units = "meters", "meters"

    # CF conventions are coordinates for center pixels
    # assign domain coordinates and shift to center
    data_dict = {
        "time": {"dims": ("time"), "data": dates},
        x_name: {
            "dims": (x_name),
            "data": domain.x_coords + (domain.resolution / 2),
            "attrs": {"long_name": x_long, "units": x_units},
        },
        y_name: {
            "dims": (y_name),
            "data": domain.y_coords - (domain.resolution / 2),
            "attrs": {"long_name": y_long, "units": y_units},
        },
    }

    bandnames = series[0].dtype.names
    series_shp = (n_imgs, domain.y_size, domain.x_size)

    for i in range(n_imgs):
        for band in bandnames:
            if i == 0:
                data_dict[band] = {
                    "dims": ("time", y_name, x_name),
                    "data": np.zeros(series_shp),
                }
            data_dict[band]["data"][i, :, :] = series[i][band][:, :]

    ds = xr.Dataset.from_dict(data_dict)

    if no_data_value is not None:
        ds = ds.where(ds != no_data_value)

    if apply_mask:
        ds = ds.where(domain.mask == 1)

    return ds


def img_to_ndarray(
    session: EESession,
    domain: Domain,
    image: ee.Image,
    bands: Iterable = None,
):
    """Function to request ee.Image as a numpy.ndarray

    args:
        session (EESession): Earth Engine cloud session used to manage REST API requests
        domain (Domain): Domain object defining the spatial region to request image
        image (ee.Image): computed ee.Image object to request
        bands (Iterable[str]): list or tuple or band names to request from image, if None then
            all bands will be requested. default = None

    returns:
        numpy.ndarray: structured numpy array where each band from image is a named field

    example:
        >>> img = (
                ee.ImageCollection('MODIS/006/MOD13Q1')
                .select("NDVI")
                .first()
            )
        >>> states = ee.FeatureCollection('TIGER/2018/States')
        >>> maine = states.filter(ee.Filter.eq('NAME', 'Maine'))
        >>> domain = restee.Domain.from_ee_geometry(session,maine,0.01)
        >>> ndvi_arr = ree.img_to_ndarray(session,domain,img)
    """
    if bands is None:
        bands = get_value(session, image.bandNames())

    pixels = _get_image(session, domain, image, bands, dataformat="NPY")

    return np.load(BytesIO(pixels))


def img_to_geotiff(
    session: EESession,
    domain: Domain,
    image: ee.Image,
    outfile: str,
    bands: Iterable = None,
):
    """Function to save requested ee.Image to a file in a GeoTIFF format

    args:
        session (EESession): Earth Engine cloud session used to manage REST API requests
        domain (Domain): Domain object defining the spatial region to request image
        image (ee.Image): computed ee.Image object to request
        outfile (str): path to write requested data to
        bands (Iterable[str]): list or tuple or band names to request from image, if None then
            all bands will be requested. default = None

    example:
        >>> img = (
                ee.ImageCollection('MODIS/006/MOD13Q1')
                .select("NDVI")
                .first()
            )
        >>> states = ee.FeatureCollection('TIGER/2018/States')
        >>> maine = states.filter(ee.Filter.eq('NAME', 'Maine'))
        >>> domain = restee.Domain.from_ee_geometry(session,maine,0.01)
        >>> ree.img_to_geotiff(session,domain,img,"maine_ndvi.tiff")
    """

    outfile = Path(outfile)

    if bands is None:
        bands = get_value(session, image.bandNames())

    pixels = _get_image(session, domain, image, bands, dataformat="GEO_TIFF")

    outfile.write_bytes(pixels)

    return


def _get_image(
    session: EESession,
    domain: Domain,
    image: ee.Image,
    bands: Iterable = None,
    dataformat: str = "NPY",
):
    """Base function to request ee.Image object for a specified domain

    args:
        session (EESession): Earth Engine cloud session used to manage REST API requests
        domain (Domain): Domain object defining the spatial region to request image
        image (ee.Image): computed ee.Image object to request
        bands (Iterable[str]): list or tuple or band names to request from image, if None then
            all bands will be requested. default = None
        dataformat (str): data format to return compute image data for domain. current options are
            'NPY' or 'GEO_TIFF'. default = 'NPY'

    returns:
        bytes: raw bytes in the format specified by dataformat

    raises:
        RequestException: when request status code is not 200
        NotImplementedError: when defined dataformat is not "NPY" or "GEO_TIFF"
    """
    project = session.cloud_project
    if bands is None:
        bands = get_value(session, project, image.bandNames())

    url = f"https://earthengine.googleapis.com/v1beta/projects/{project}/image:computePixels"

    serialized = ee.serializer.encode(image, for_cloud_api=True)

    payload = dict(
        expression=serialized,
        fileFormat=dataformat,
        bandIds=bands,
        grid=domain.pixelgrid,
    )

    response = session.send_request(url, payload)

    if response.status_code != 200:
        raise requests.exceptions.RequestException(
            f"received the following bad status code: {response.status_code}\nServer message: {response.json()['error']['message']}"
        )

    if dataformat in ["NPY", "GEO_TIFF"]:
        result = response.content
    elif dataformat == "TF_RECORD_IMAGE":
        raise NotImplementedError()
    else:
        raise NotImplementedError(
            f"select dataformat {dataformat} is not implemented.Options are 'NPY','GEO_TIFF', or 'TF_RECORD_IMAGE'"
        )

    return result
