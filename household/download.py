"""
Open Power System Data

Household Datapackage

download.py : download households sources

"""
import logging
import os
import zipfile

import requests


logger = logging.getLogger('log')
logger.setLevel('INFO')


def download(version=None):
    """
    Download archived data from the OPSD server.

    Parameters
    ----------
    version: str
        OPSD Data Package Version to download original data from.

    Returns
    ----------
    None

    """

    filepath = 'original_data.zip'

    if not os.path.exists(filepath):
        url = ('http://data.open-power-system-data.org/household/'
               '{}/original_data/{}'.format(version, filepath))
        logger.info('Downloading and extracting archived data from %s', url)
        resp = requests.get(url)
        with open(filepath, 'wb') as output_file:
            for chunk in resp.iter_content(1024):
                output_file.write(chunk)

    else:
        logger.info('%s already exists. Delete it if you want to download again',
                    filepath)

        myzipfile = zipfile.ZipFile(filepath)
        if myzipfile.namelist()[0] == 'original_data/':
            myzipfile.extractall()
            logger.info('Extracted data to /original_data.')
        else:
            logger.warning('%s has unexpected content. Please check manually',
                           filepath)

    return

