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

S3_REL = 'http://esipfed.org/ns/fedsearch/1.1/s3#'

ED_USERNAME = os.environ.get('EARTHDATA_USERNAME')
ED_PASSWORD = os.environ.get('EARTHDATA_PASSWORD')
auth_mode = os.environ.get('AUTH_MODE', 'edl')

if auth_mode not in ('edl', 'iamrole'):
    raise ValueError(f'Unsupported auth mode: {auth_mode}')

CREDENTIALS_API = 'https://archive.podaac.earthdata.nasa.gov/s3credentials'
SHORT_NAME = 'MUR-JPL-L4-GLOB-v4.1'
CONCAT_DIMS = ['time']
IDENTICAL_DIMS = ['lat', 'lon']
SELECTED_VARS = ['analysed_sst', 'analysis_error', 'mask', 'sea_ice_fraction']

missing_date_strings = ['2021-02-20', '2021-02-21', '2022-11-09']
missing_dates = pd.to_datetime(missing_date_strings)
dates = [
    d.to_pydatetime().strftime('%Y%m%d')
    for d in pd.date_range('2002-06-01', '2023-02-23', freq='D')
    if d not in missing_dates
]


def make_filename(time):
    base_url = f's3://podaac-ops-cumulus-protected/{SHORT_NAME}/'
    # example file: "/20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"
    return f'{base_url}{time}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc'


concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_filename, concat_dim)

def get_s3_creds(username: str = None, password: str = None, credentials_api: str = CREDENTIALS_API):
    if auth_mode == 'iamrole':
        import boto3
        client = boto3.client('sts')
        creds = client.assume_role(
            RoleArn=os.environ.get('AWS_ROLE_ARN'),
            RoleSessionName='mursst-pangeo-forge',
        )['Credentials']
        return {
            'key': creds['AccessKeyId'],
            'secret': creds['SecretAccessKey'],
            'token': creds['SessionToken'],
            'anon': False,
        }
    elif auth_mode == 'edl': 
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
        #import pdb; pdb.set_trace()
        results = requests.get(CREDENTIALS_API, cookies={'accessToken': final.cookies['accessToken']})
        #import pdb; pdb.set_trace()
        results.raise_for_status()

        creds = json.loads(results.content)
        return {
            'key': creds['accessKeyId'],
            'secret': creds['secretAccessKey'],
            'token': creds['sessionToken'],
            'anon': False,
        }

fsspec_open_kwargs = get_s3_creds(ED_USERNAME, ED_PASSWORD)
    
recipe = (
    beam.Create(pattern.items())
    | OpenWithKerchunk(
        remote_protocol='s3',
        file_type=pattern.file_type,
        # lat/lon are around 5k, this is the best option for forcing kerchunk to inline them
        inline_threshold=6000,
        storage_options=fsspec_open_kwargs,
    )
    | WriteCombinedReference(
        concat_dims=CONCAT_DIMS,
        identical_dims=IDENTICAL_DIMS,
        store_name=SHORT_NAME,
        remote_options=fsspec_open_kwargs,
        remote_protocol='s3',
        mzz_kwargs={'coo_map': {"time": "cf:time"}, 'inline_threshold': 0}
    )
)
