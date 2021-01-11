import ee
import json
import backoff
import requests
import numpy as np
from affine import Affine
from rasterio import features
from collections.abc import Iterable

from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

# from . import tables as eetables

class Domain:
    """Domain class to define spatial region for requests
    """
    def __init__(self, 
        bbox: Iterable, 
        resolution: float = 0.25, 
        crs: str = "EPSG:4326"
    ):
        # set crs
        self.crs = crs

        # set resolution info
        self.resolution = resolution
            
        # set the bounding box
        if np.any((np.array(bbox) % self.resolution)) != 0:
            self.bbox = _round_out(bbox,self.resolution)
        else:
            self.bbox = bbox

        minx, miny, maxx, maxy = self.bbox
        y_coords = np.arange(miny, maxy + self.resolution, self.resolution)[::-1]
        x_coords = np.arange(minx, maxx + self.resolution, self.resolution)

        self.x_size = x_coords.size
        self.y_size = y_coords.size

        self.x_coords = x_coords
        self.y_coords = y_coords
        
        self.xx, self.yy = np.meshgrid(x_coords,y_coords)

        self.shape = self.xx.shape

        self.transform = Affine.from_gdal(*(minx,self.resolution,0.0,maxy,0.0,-self.resolution))

        self._mask = np.ones(self.shape)

        return

    @property
    def pixelgrid(self):
        """Property for json/dict represenstion of pixel grid info for requesting EE rasters

        """
        at_keys = ("translateX","scaleX","shearX","translateY","shearY","scaleY")
        gt = self.transform.to_gdal()
        affinetranform = {k:gt[i] for i,k in enumerate(at_keys)}

        dims = dict(width = self.x_size, height = self.y_size)

        pgrid = dict(
            affineTransform = affinetranform,
            dimensions = dims,
            crsCode = self.crs
        )
        
        return pgrid

    @property
    def mask(self):
        return self._mask

    @mask.setter
    def mask(self,value):
        if value.shape == self.shape:
            self._mask = value
        else:
            raise AttributeError(f"provided mask has a shape of {value.shape} which does not match the domain shape of {self.shape}")
        return

    def to_ee_geometry(self):
        return ee.Geometry.Rectangle(self.bbox)


    @staticmethod
    def from_geopandas(gdf, resolution: float = 0.25):
        bbox = _round_out(gdf.total_bounds,res=resolution)
        crs = gdf.crs.srs
        d = Domain(bbox,resolution,crs)
        d.mask = features.geometry_mask(
            gdf.geometry, d.shape, transform=d.transform, all_touched=True, invert=True)
        return d


    @staticmethod
    def from_rasterio(ds,mask_val=None):
        resolution = np.mean(ds.res)
        crs = ds.crs.data["init"]
        bbox = tuple(ds.bounds)
        d = Domain(bbox,resolution,crs)
        #TODO: add code to automatically mask no data values
        return d

    # @staticmethod
    # def from_ee_geometry(session,project,geom,resolution: float = 0.25):
    #     if isinstance(geom,ee.Geometry):
    #         fc = ee.FeatureCollection([ee.Feature(geom)])
    #     elif isinstance(geom,ee.Feature):
    #         fc = ee.FeatureCollection([geom])
        
    #     gdf = eetables.fccollection_to_geopandas(session,project,fc)

    #     return Domain.from_geopandas(gdf)


    @staticmethod
    def from_xarray(ds, xdim: str = "lon", ydim: str = "lat"):

        return Domain()


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


def _fatal_code(e):
    # return 400 <= e.response.status_code < 500
    return e.response.status_code == 404


@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=5,
                      max_time=300,
                      giveup=_fatal_code)
def _send_request(session,url,data):
    return session.post(url=url, data=json.dumps(data))


def get_session(key):
    """Helper function to authenticate 
    """
    credentials = service_account.Credentials.from_service_account_file(key)
    scoped_credentials = credentials.with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform'])

    return AuthorizedSession(scoped_credentials)