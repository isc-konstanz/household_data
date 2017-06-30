"""
Open Power System Data

Household Datapackage

read.py : read time series files

"""
from datetime import datetime, timedelta
import logging
import os
from struct import unpack
import sys
import zipfile

import pytz
import requests

import numpy as np
import pandas as pd


logger = logging.getLogger('log')
logger.setLevel('INFO')


def read(household_name, dir_name, feeds, project, household_region, household_type, headers, 
         out_path='original_data', start_from_user=None, end_from_user=None):
    """
    For the households specified in the households.yml file, read 

    Parameters
    ----------
    feeds : dict of int
        Indicator for subset of feed ids, available for the Household
    dir_name : str
        directory path to the location of the Households MySQL data
    household_name : str
        Name of the Household to be placed in the column-MultiIndex
    project : str
        Project name of the Household to be placed in the column-MultiIndex
    household_region : str
        Region of the Household to be placed in the column-MultiIndex
    household_type : str
        Type of the Household to be placed in the column-MultiIndex
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    out_path : str, default: 'original_data'
        Base download directory in which to save all downloaded files
    start_from_user : datetime.date, default None
        Start of period for which to read the data
    end_from_user : datetime.date, default None
        End of period for which to read the data

    Returns
    ----------
    data_set: pandas.DataFrame
        A DataFrame containing the combined data for household 

    """
    data_set = pd.DataFrame()
    columns_map = {}

    feeds_dir = os.path.join('original_data', dir_name, 'phptimeseries')

    logger.info('Reading %s - feeds', household_name)

    feeds_existing = len(feeds)
    feeds_success = 0

    # Check if there is a feeds folder for dir_name
    if not os.path.exists(feeds_dir):
        logger.warning('Feeds directory not found for %s',
                       dir_name)
        return data_set

    # For each specified feed, read the MySQL file
    for feed_name, feed_id in feeds.items():

        filepath = os.path.join(feeds_dir, 'feed_'+str(feed_id)+'.MYD')

        # Check if file is not empty
        if os.path.getsize(filepath) < 128:
            logger.warning('%s \n file is smaller than 128 Byte. It is probably'
                           ' empty and will thus be skipped from reading',
                           filepath)
        else:
            logger.debug('Reading data:\n\t '
                         'Household:   %s\n\t '
                         'Feed: %s',
                         household_name, feed_name)

            update_progress(feeds_success, feeds_existing)

            data_to_add = read_feed(filepath, feed_name)

            columns_map[feed_name] = {
                'project': project,
                'household_region': household_region,
                'household': household_name,
                'type': household_type,
                'feed': feed_name
            }

            if data_set.empty:
                data_set = data_to_add
            else:
                data_set = data_set.combine_first(data_to_add)

            feeds_success += 1
            update_progress(feeds_success, feeds_existing)

    if data_set.empty:
        logger.warning('Returned empty DataFrame for %s', household_name)
        return data_set

    # Create the MultiIndex
    tuples = [tuple(columns_map[col][level] for level in headers)
              for col in data_set.columns]
    data_set.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    # Cut off the data outside of [start_from_user:end_from_user]
    # First, convert userinput to UTC time to conform with data_set.index
#     if start_from_user:
#         start_from_user = (
#             pytz.timezone('Europe/Brussels')
#             .localize(datetime.combine(start_from_user, time()))
#             .astimezone(pytz.timezone('UTC')))
#     if end_from_user:
#         end_from_user = (
#             pytz.timezone('Europe/Brussels')
#             .localize(datetime.combine(end_from_user, time()))
#             .astimezone(pytz.timezone('UTC'))
#             # appropriate offset to inlude the end of period
#             + timedelta(days=1, minutes=-int(res_key[:2])))
#     # Then cut off the data_set
#     data_set = data_set.loc[start_from_user:end_from_user, :]

    return data_set


def read_feed(filepath, name):
    times = []
    data = []
    with open(filepath, 'rb') as file:
        line = file.read(9)
        while line:
            line_tuple = unpack("<xIf", line)
            
            timestamp = int(line_tuple[0])
            if timestamp > 0:
                times.append(datetime.fromtimestamp(timestamp))
                
                value = float(line_tuple[1])
                data.append(value)
            
            line = file.read(9)
    
    feed = pd.DataFrame(data=data, index=times, columns=[name])
    feed.index = feed.index.tz_localize(pytz.utc)
    feed.index.name = 'timestamp'
    feed = feed.loc[feed.index.year > 1970]
    feed -= feed.dropna().iloc[0]

    # Keep only the rows where the energy values is increasing
    error = feed.shift(-1) < feed
    error = error | error.shift(1).fillna(False) | error.shift(2).fillna(False)
    if np.count_nonzero(error) > 0:
        feed = feed[~error]
        
        logger.debug("Dropped %s rows with decreasing energy in %s (%s)", str(np.count_nonzero(error)), filepath, name)

    index_delta = feed.index[1:-1] - feed.index[0:-2]
    index_delta.name = 'delta'
    outage = pd.DataFrame(data=index_delta[index_delta.seconds > 15*60], index=feed.index[0:-2][index_delta.seconds > 15*60])

    # Extend index to have a regular frequency
    start = feed.index[0].replace(minute=feed.index[0].minute+1, second=0)
    end = feed.index[-1].replace(minute=feed.index[-1].minute-1, second=0)
    index = pd.DatetimeIndex(start=start, end=end, freq='1min')

    feed = feed.combine_first(pd.DataFrame(index=index, columns=[name]))

    # Drop rows with duplicate index, as this produces problems with reindexing
    feed = feed[~feed.index.duplicated(keep='last')]
    
    # Drop rows with outages longer than 15 minutes
    for i in outage.index:
        error = feed[i:i+timedelta(seconds=abs(outage.loc[i, 'delta'].seconds))][1:-2]
        feed = feed.drop(error.index)

    # Interpolate the values between the irregular data points and drop them afterwards, 
    # to receive a regular index that is sure to be continuous, in order to later expose 
    # remaining gaps in the data.
    feed = feed.interpolate()
    feed = feed.reindex(index=index)

    return feed


def update_progress(count, total):
    '''
    Display or updates a console progress bar.

    Parameters
    ----------
    count : int
        number of feeds that have been read so far
    total : int
        total number of feeds

    Returns
    ----------
    None

    '''

    barLength = 50  # Modify this to change the length of the progress bar
    status = ""
    progress = count / total
    if isinstance(progress, int):
        progress = float(progress)
    if progress >= 1:
        progress = 1
        status = "Done...\r\n"
    block = int(round(barLength * progress))
    text = "\rProgress: [{0}] {1}/{2} feeds {3}".format(
        "#" * block + "-" * (barLength - block), count, total, status)
    sys.stdout.write(text)
    sys.stdout.flush()

    return

