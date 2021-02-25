# restee Installation

This pages walks you through setting up the `restee` package as well as points users to the documentation for setting up a GCP project for use with the EE REST API.

## Before beginning

Before proceeding with the package installation, please see the instructions to setup a service account for access to the REST API. Instructions for setting up your service account can be found [here](https://developers.google.com/earth-engine/reference/Quickstart#before-you-begin)

Once you have a Google Cloud Project and whitelisted service accout for the cloud project, you will need to create a private key so that your system can securely communicate with the Google Cloud. Instructions for creating a private key can be found [here](https://developers.google.com/earth-engine/reference/Quickstart#obtain-a-private-key-file-for-your-service-account)

Lastly, test the setup by following the instructions [here](https://developers.google.com/earth-engine/reference/Quickstart#accessing-and-testing-your-credentials)

## Installing the package

`restee` relies heavily on the geospatial Python ecosystem to manage different geospatial data formats and execute geospatial processes. It is recommended to use [`conda`](https://docs.anaconda.com/anaconda/install/) to handle the package dependencies and create a virtual environment to work with `restee`. To do this run the following command:

```sh
conda create -n restee -c conda-forge -y \
    python>=3.6 \
    numpy \
    scipy \
    pandas \
    xarray \
    rasterio \
    geopandas \
    pyproj \
    requests \
    backoff \
    earthengine-api \
    tqdm
```

Once all of the dependencies are installed, the `restee` package can be installed using `pip`:

```sh
pip install restee
```
