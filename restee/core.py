import ee
import copy
import json
import backoff
import requests
import numpy as np
from io import StringIO
import geopandas as gpd
from affine import Affine
from rasterio import features
from pyproj import Transformer
from collections.abc import Iterable
from scipy import interpolate, ndimage

from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account


class Domain:
    """Domain class to define spatial region for image requests

    example:
        Defines a psuedo-global domain at 1 degree resolution
        >>> coords =  [-180,-60,180,85]
        >>> domain = restee.Domain(coords,resolution=1)
        >>> domain.shape
            (145, 360)
    """

    def __init__(
        self, bbox: Iterable, resolution: float = 0.25, crs: str = "EPSG:4326"
    ):
        """Initialize Domain class

        args:
            bbox (Iterable): bounding box to create domain as [W,S,E,N]
            resolution (float): resolution to make domain. default = 0.25
            crs (str): string name of coordinate reference system of domain.
                resolution units must match the crs units. default = "EPSG:4326"
        """
        # set crs
        self._crs = crs

        # set resolution info
        self.resolution = resolution

        # set the bounding box
        if np.any((np.array(bbox) % self.resolution)) != 0:
            self.bbox = Domain._round_out(bbox, self.resolution)
        else:
            self.bbox = bbox

        minx, miny, maxx, maxy = self.bbox
        y_coords = np.arange(miny+self.resolution, maxy+self.resolution, self.resolution)[::-1]
        x_coords = np.arange(minx, maxx, self.resolution)

        self.x_size = x_coords.size
        self.y_size = y_coords.size

        self.x_coords = x_coords
        self.y_coords = y_coords

        self._construct_grid()

        self._mask = np.ones(self.shape)

        return

    def _construct_grid(self):
        """Helper function to create a grid of coordinates and geotransform"""

        self.xx, self.yy = np.meshgrid(self.x_coords, self.y_coords)

        self.shape = self.xx.shape

        self.transform = Affine.from_gdal(
            *(self.bbox[0], self.resolution, 0.0, self.bbox[-1], 0.0, -self.resolution)
        )

        return

    @property
    def pixelgrid(self):
        """Property for json/dict represenstion of pixel grid info for requesting EE rasters.
        Used to define the spatial domain and shape of imagery for requests

        returns:
            dict: dictionary representation of pixel grid (https://developers.google.com/earth-engine/reference/rest/v1beta/PixelGrid)
        """
        at_keys = ("translateX", "scaleX", "shearX", "translateY", "shearY", "scaleY")
        gt = self.transform.to_gdal()
        affinetranform = {k: gt[i] for i, k in enumerate(at_keys)}

        dims = dict(width=self.x_size, height=self.y_size)

        pgrid = dict(affineTransform=affinetranform, dimensions=dims, crsCode=self.crs)

        return pgrid

    @property
    def crs(self):
        return self._crs

    @property
    def mask(self):
        return self._mask

    @mask.setter
    def mask(self, value):
        if value.shape == self.shape:
            self._mask = value
        else:
            raise AttributeError(
                f"provided mask has a shape of {value.shape} which does not match the domain shape of {self.shape}"
            )
        return

    def to_ee_bbox(self):
        """Converts the domain bounding box to and ee.Geometry

        returns:
            ee.Geometry: bounding box of domain
        """
        return ee.Geometry.Rectangle(self.bbox)

    def resample(self, factor: float):
        """Function to resample domain shape and coordinates to a new resolution.
        Useful for requesting imagery over the same domain but at different spatial
        resolutions

        args:
            factor (float): factor to scale the shape and coordinates. For example,
                if factor = 2, then the resolution will half and shape doubles.

        returns:
            restee.Domain: domain object with new resolution/coordinates
        """
        new = copy.deepcopy(self)
        new.resolution = float(new.resolution / factor)
        # interpolate the x coordinates
        old_x = np.arange(self.x_size) * factor
        new_x = np.arange(self.x_size * factor)
        f_x = interpolate.interp1d(
            old_x, self.x_coords, bounds_error=False, fill_value="extrapolate"
        )
        new.x_coords = f_x(new_x)

        old_x = np.arange(self.y_size) * factor
        new_x = np.arange(self.y_size * factor)
        f_y = interpolate.interp1d(
            old_x, self.y_coords, bounds_error=False, fill_value="extrapolate"
        )
        new.y_coords = f_y(new_x)

        new.x_size = new.x_coords.size
        new.y_size = new.y_coords.size

        new._construct_grid()

        interp_mask = ndimage.zoom(self.mask, factor, order=0, mode="nearest")
        new.mask = interp_mask.astype(np.bool)

        return new

    @staticmethod
    def from_geopandas(gdf, resolution: float = 0.25):
        """Domain constructor function that takes a GeoDataFrame and returns a domain
        object with the vector as a mask.

        args:
            gdf (geopandas.GeoDataFrame): GeoDataFrame to create the domain from
            resolution (float): resolution to make domain, must match units of vector crs.
                default = 0.25

        returns:
            restee.Domain: domain object with mask from vector
        """
        bbox = Domain._round_out(gdf.total_bounds, res=resolution)
        crs = gdf.crs.srs
        d = Domain(bbox, resolution, crs)
        d.mask = features.geometry_mask(
            gdf.geometry, d.shape, transform=d.transform, all_touched=True, invert=True
        )
        return d

    @staticmethod
    def from_rasterio(ds, mask_val=None):
        """Domain contructor function that takes a rasterio object and returns a domain
        with the same geotransform and crs

        args:
            ds (rasterio): rasterio object to model domain from

        returns:
            restee.Domain: domain object with same geotransform and crs as the input
        """
        resolution = np.mean(ds.res)
        crs = ds.crs.data["init"]
        bbox = tuple(ds.bounds)
        d = Domain(bbox, resolution, crs)
        # TODO: add code to automatically mask no data values
        return d

    @staticmethod
    def from_ee_geometry(session, geom, resolution: float = 0.25):
        """Domain contructor function that takes a ee.Geometry, ee.Feature, or ee.FeatureCollection
        and returns a domain object with the geometry as a mask. Useful for using ee to process
        a region and use as domain for requsting imagery.

        args:
            session (EESession): restee session autheticated to make requests
            geom (ee.Geometry|ee.Feature|ee.FeatureCollection): ee object to create the domain from
            resolution (float): resolution to make domain, must match units of vector crs.
                default = 0.25

        returns:
            restee.Domain: domain object with mask from vector
        """
        if isinstance(geom, ee.Geometry):
            fc = ee.FeatureCollection([ee.Feature(geom)])
        elif isinstance(geom, ee.Feature):
            fc = ee.FeatureCollection([geom])
        else:
            fc = geom

        project = session.cloud_project
        url = f"https://earthengine.googleapis.com/v1beta/projects/{project}/table:computeFeatures"
        serialized = ee.serializer.encode(fc, for_cloud_api=True)
        payload = dict(expression=serialized)

        response = session.send_request(url, payload)

        gdf = gpd.read_file(StringIO(response.content.decode()))

        return Domain.from_geopandas(gdf, resolution=resolution)

    # TODO: write a Domain constructor from xarray dataarray/dataset

    @staticmethod
    def _round_out(bb: Iterable, res: float):
        """Function to round bounding box to nearest resolution
        args:
            bb (iterable): list of bounding box coordinates in the order of [W,S,E,N]
            res: (float): resolution of pixels to round bounding box to
        """
        minx = bb[0] - (bb[0] % res)
        miny = bb[1] - (bb[1] % res)
        maxx = bb[2] + (res - (bb[2] % res))
        maxy = bb[3] + (res - (bb[3] % res))
        return minx, miny, maxx, maxy


# @staticmethod
def _fatal_code(e):
    """Helper function defining a fatal error for backoff decorator to stop requests
    """
    # return 400 <= e.response.status_code < 500
    return e.response.status_code == 404


class EESession:
    """EESession class that handles GCP/EE REST API info to make authenticated requests to Google Cloud.
    Users provides credentials that are used to create an authorized session to make HTTP requests

    """
    def __init__(self, project: str, key: str):
        """Initialization function for the EESession class

        args:
            project (str): Google Cloud project name with service account whitelisted to use Earth Engine
            key (str): path to private key file for your whitelisted service account
        """
        self._PROJECT = project
        self._SESSION = self._get_session(key)

    @property
    def cloud_project(self):
        return self._PROJECT

    @property
    def session(self):
        return self._SESSION

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
        max_time=300,
        giveup=_fatal_code,
    )
    def send_request(self, url, data):
        """Method to send authenticated requests to google cloud.
        This is wrapped with a backoff decorator that will try multiple requests
        if the initial ones fail.

        args:
            url (str): EE REST API endpoint to send request.
                See https://developers.google.com/earth-engine/reference/rest for more info
            data (dict): Dictionary object to send in the body of the Request.

        returns:
            response: Reponse object with information on status and content
        """
        return self.session.post(url=url, data=json.dumps(data))

    @staticmethod
    def _get_session(key):
        """Helper function to authenticate"""
        credentials = service_account.Credentials.from_service_account_file(key)
        scoped_credentials = credentials.with_scopes(
            ["https://www.googleapis.com/auth/cloud-platform"]
        )

        return AuthorizedSession(scoped_credentials)
