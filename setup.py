import setuptools
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='restee',
      version='0.0.3',
      description='Python package to call processed EE objects via the REST API to local data',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='http://github.com/kmarkert/restee',
      packages=setuptools.find_packages(),
      author='Kel Markert',
      author_email='kel.markert@gmail.com',
      license='MIT',
      zip_safe=False,
      include_package_data=True,
      install_requires=[
        'numpy',
        'scipy',
        'pandas',
        'xarray',
        'rasterio',
        'geopandas',
        'pyproj',
        'requests',
        'backoff',
        'earthengine-api',
        'tqdm',
    ],
)
