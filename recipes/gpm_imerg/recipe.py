import base64
import json
import os
from dataclasses import dataclass, field
from typing import Dict, Set

import apache_beam as beam
import pandas as pd
import requests
import xarray as xr
import zarr
from requests.auth import HTTPBasicAuth

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import OpenWithKerchunk, WriteCombinedReference

HTTP_REL = 'http://esipfed.org/ns/fedsearch/1.1/data#'
S3_REL = 'http://esipfed.org/ns/fedsearch/1.1/s3#'

ED_USERNAME = os.environ['EARTHDATA_USERNAME']
ED_PASSWORD = os.environ['EARTHDATA_PASSWORD']
earthdata_protocol = os.environ['PROTOCOL'] or 'https'

if earthdata_protocol not in ('https', 's3'):
    raise ValueError(f'Unknown ED_PROTOCOL: {earthdata_protocol}')

CREDENTIALS_API = 'https://data.gesdisc.earthdata.nasa.gov/s3credentials'
SHORT_NAME = 'GPM_3IMERGDF.07'
CONCAT_DIMS = ['time']
IDENTICAL_DIMS = ['lat', 'lon']

# 2023/07/3B-DAY.MS.MRG.3IMERG.20230731
dates = [
    d.to_pydatetime().strftime('%Y/%m/3B-DAY.MS.MRG.3IMERG.%Y%m%d')
    for d in pd.date_range('2000-06-01', '2000-06-05', freq='D')
]


def make_filename(time):
    if earthdata_protocol == 'https':
        # https://data.gesdisc.earthdata.nasa.gov/data/GPM_L3/GPM_3IMERGDF.07/2023/07/3B-DAY.MS.MRG.3IMERG.20230731-S000000-E235959.V07B.nc4
        base_url = f'https://data.gesdisc.earthdata.nasa.gov/data/GPM_L3/{SHORT_NAME}/'
    else:
        base_url = f's3://gesdisc-cumulus-prod-protected/GPM_L3/{SHORT_NAME}/'
    return f'{base_url}{time}-S000000-E235959.V07B.nc4'


concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_filename, concat_dim)


def get_earthdata_token(username, password):
    # URL for the Earthdata login endpoint
    login_url = 'https://urs.earthdata.nasa.gov/api/users/token'
    auth = HTTPBasicAuth(username, password)

    # Request a new token
    response = requests.get(f'{login_url}s', auth=auth)

    # Check if the request was successful
    if response.status_code == 200:
        if len(response.json()) == 0:
            # create new token
            response = requests.post(login_url, auth=auth)
            if response.status_code == 200:
                token = response.json()['access_token']
            else:
                raise Exception('Error: Unable to generate Earthdata token.')
        else:
            # Token is usually in the response's JSON data
            token = response.json()[0]['access_token']
        return token
    else:
        raise Exception('Error: Unable to retrieve Earthdata token.')


def get_s3_creds(username, password, credentials_api=CREDENTIALS_API):
    login_resp = requests.get(CREDENTIALS_API, allow_redirects=False)
    login_resp.raise_for_status()

    encoded_auth = base64.b64encode(f'{username}:{password}'.encode('ascii'))
    auth_redirect = requests.post(
        login_resp.headers['location'],
        data={'credentials': encoded_auth},
        headers={'Origin': credentials_api},
        allow_redirects=False,
    )
    auth_redirect.raise_for_status()

    final = requests.get(auth_redirect.headers['location'], allow_redirects=False)

    results = requests.get(CREDENTIALS_API, cookies={'accessToken': final.cookies['accessToken']})
    results.raise_for_status()

    creds = json.loads(results.content)
    return {
        'key': creds['accessKeyId'],
        'secret': creds['secretAccessKey'],
        'token': creds['sessionToken'],
        'anon': False,
    }


def earthdata_auth(username: str, password: str):
    if earthdata_protocol == 's3':
        return get_s3_creds(username, password)
    else:
        token = get_earthdata_token(username, password)
        return {'headers': {'Authorization': f'Bearer {token}'}}


fsspec_open_kwargs = earthdata_auth(ED_USERNAME, ED_PASSWORD)


# Remove method when https://github.com/pangeo-forge/pangeo-forge-recipes/pull/556/files is merged.
@beam.ptransform_fn
def ConsolidateMetadata(pcoll: beam.PCollection) -> beam.PCollection:
    """Consolidate metadata into a single .zmetadata field.
    See zarr.consolidate_metadata() for details.
    """

    def _consolidate(store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        zarr.consolidate_metadata(store, path=None)
        return store

    return pcoll | beam.Map(_consolidate)


@dataclass
class ValidateDatasetDimensions(beam.PTransform):
    """Open the reference.json in xarray and validate dimensions."""

    expected_dims: Dict = field(default_factory=dict)

    @staticmethod
    def _validate(zarr_store: zarr.storage.FSStore, expected_dims: Dict) -> None:
        ds = xr.open_dataset(zarr_store, engine='zarr')
        if set(ds.dims) != expected_dims.keys():
            raise ValueError(f'Expected dimensions {expected_dims.keys()}, got {ds.dims}')
        for dim, bounds in expected_dims.items():
            if bounds is None:
                continue
            lo, hi = bounds
            actual_lo, actual_hi = round(ds[dim].data.min()), round(ds[dim].data.max())
            if actual_lo != lo or actual_hi != hi:
                raise ValueError(f'Expected {dim} range [{lo}, {hi}], got {actual_lo, actual_hi}')
        return ds

    def expand(
        self,
        pcoll: beam.PCollection,
    ) -> beam.PCollection:
        return pcoll | beam.Map(self._validate, expected_dims=self.expected_dims)


recipe = (
    beam.Create(pattern.items())
    | OpenWithKerchunk(
        remote_protocol=earthdata_protocol,
        file_type=pattern.file_type,
        # lat/lon are around 5k, this is the best option for forcing kerchunk to inline them
        inline_threshold=6000,
        storage_options=fsspec_open_kwargs,
    )
    | WriteCombinedReference(
        concat_dims=CONCAT_DIMS,
        identical_dims=IDENTICAL_DIMS,
        store_name=SHORT_NAME,
        target_options=fsspec_open_kwargs,
        remote_options=fsspec_open_kwargs,
        remote_protocol=earthdata_protocol,
    )
    | ConsolidateMetadata()
    | ValidateDatasetDimensions(expected_dims={'time': None, 'lat': (-90, 90), 'lon': (-180, 180), 'nv': (0, 1)})
)