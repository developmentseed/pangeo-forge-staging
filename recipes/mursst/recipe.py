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

ED_USERNAME = os.environ.get('EARTHDATA_USERNAME')
ED_PASSWORD = os.environ.get('EARTHDATA_PASSWORD')
auth_mode = os.environ.get('AUTH_MODE', 'edl')
aws_role_arn = os.environ.get('AWS_ROLE_ARN')

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
    for d in pd.date_range('2002-06-01', '2005-06-01', freq='D')
    if d not in missing_dates
]


def make_filename(time):
    base_url = f's3://podaac-ops-cumulus-protected/{SHORT_NAME}/'
    # example file: "/20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"
    return f'{base_url}{time}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc'


concat_dim = ConcatDim('time', dates, nitems_per_file=1)
pattern = FilePattern(make_filename, concat_dim)

class GetS3Creds(beam.DoFn):
    def process(self, element):
        if all(k in os.environ for k in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN')):
            print("Credentials already set")
            yield element
            
        if auth_mode == 'iamrole':
            # If AWS_ROLE_ARN is none, it should be that the current process
            # is already assuming or using a role that has access to the data,
            # as in the case of a NASA JupyterHub like hub.openveda.cloud.
            #
            # If AWS_ROLE_ARN is NOT none, it should be that the current process
            # can assume the role and the role has access to the data.
            if aws_role_arn == None:
                yield element
            else:             
                import boto3
                client = boto3.client('sts')
                creds = client.assume_role(
                    RoleArn=aws_role_arn,
                    RoleSessionName='mursst-pangeo-forge',
                    DurationSeconds=43200,
                )['Credentials']
                os.environ['AWS_ACCESS_KEY_ID'] = creds['AccessKeyId']
                os.environ['AWS_SECRET_ACCESS_KEY'] = creds['SecretAccessKey']
                os.environ['AWS_SESSION_TOKEN'] = creds['SessionToken']
                print(f"Credentials set via assumed IAM Role {aws_role_arn}")
                yield element
        elif auth_mode == 'edl': 
            login_resp = requests.get(CREDENTIALS_API, allow_redirects=False)
            login_resp.raise_for_status()

            encoded_auth = base64.b64encode(f'{ED_USERNAME}:{ED_PASSWORD}'.encode('ascii'))
            auth_redirect = requests.post(
                login_resp.headers['location'],
                data={'credentials': encoded_auth},
                headers={'Origin': CREDENTIALS_API},
                allow_redirects=False,
            )
            auth_redirect.raise_for_status()

            final = requests.get(auth_redirect.headers['location'], allow_redirects=False)
            results = requests.get(CREDENTIALS_API, cookies={'accessToken': final.cookies['accessToken']})
            results.raise_for_status()

            creds = json.loads(results.content)
            os.environ['AWS_ACCESS_KEY_ID'] = creds['accessKeyId']
            os.environ['AWS_SECRET_ACCESS_KEY'] = creds['secretAccessKey']
            os.environ['AWS_SESSION_TOKEN'] = creds['sessionToken']
            print("Credentials set via Earthdata Login")
            yield element
    
recipe = (
    beam.Create(pattern.items())
    | "Set AWS Credentials" >> beam.ParDo(GetS3Creds())
    | OpenWithKerchunk(
        remote_protocol='s3',
        file_type=pattern.file_type,
        # lat/lon are around 5k, this is the best option for forcing kerchunk to inline them
        inline_threshold=6000,
        storage_options={'anon': False },
    )
    | WriteCombinedReference(
        concat_dims=CONCAT_DIMS,
        identical_dims=IDENTICAL_DIMS,
        store_name=SHORT_NAME,
        remote_options={'anon': False },
        remote_protocol='s3',
        mzz_kwargs={'coo_map': {"time": "cf:time"}, 'inline_threshold': 0}
    )
)
