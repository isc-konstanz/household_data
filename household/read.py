"""
Open Power System Data

Household Datapackage

read.py : read time series files

"""
import logging
logger = logging.getLogger(__name__)

import os
import pytz
import pandas as pd

from struct import unpack
from datetime import datetime, time
from .tools import update_progress


def read(household_name, household_dir, household_region, household_type, feeds, headers, 
         start_from_user=None, end_from_user=None):
    """
    For the households specified in the households.yml file, read 

    Parameters
    ----------
    household_name : str
        Name of the Household to be placed in the column-MultiIndex
    household_dir : str
        directory path to the location of the Households MySQL data
    household_region : str
        Region of the Household to be placed in the column-MultiIndex
    household_type : str
        Type of the Household to be placed in the column-MultiIndex
    feeds : dict of key value pairs
        Indicator for subset of feed ids, available for the Household
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
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

    household_id = household_name.replace(' ', '').lower()
    feeds_dir = os.path.join('original_data', household_dir, 'phptimeseries')

    logger.info('Reading %s series', household_name)

    feeds_existing = len(feeds)
    feeds_success = 0

    # Check if there is a feeds folder for household_dir
    if not os.path.exists(feeds_dir):
        logger.warning('Feeds directory not found for %s',
                       household_dir)
        return data_set

    # For each specified feed, read the MySQL file
    for feed_name, feed_dict in feeds.items():
        feed_id = feed_dict['id']
        feed_unit = feed_dict['unit']

        filepath = os.path.join(feeds_dir, 'feed_'+str(feed_id)+'.MYD')

        # Check if file is not empty
        if os.path.getsize(filepath) < 128:
            logger.warning('%s \n file is smaller than 128 Byte. It is probably'
                           ' empty and will thus be skipped from reading',
                           filepath)
        else:
            data_to_add = read_feed(filepath, feed_name)

            columns_map[feed_name] = {
                'region': household_region,
                'household': household_id,
                'type': household_type,
                'unit': feed_unit,
                'feed': feed_name
            }

            if data_set.empty:
                data_set = data_to_add
            else:
                data_set = data_set.combine_first(data_to_add)
            
            logger.debug('Read data series %s for %s from %s to %s',
                         household_name, feed_name,
                         data_to_add.index[0].strftime('%d.%m.%Y %H:%M'),
                         data_to_add.index[-1].strftime('%d.%m.%Y %H:%M'))
            
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
    if start_from_user:
        start_from_user = (
            pytz.timezone('Europe/Berlin')
            .localize(datetime.combine(start_from_user, time()))
            .astimezone(pytz.timezone('UTC')))
        data_set = data_set.loc[data_set.index >= start_from_user]
        
    if end_from_user:
        end_from_user = (
            pytz.timezone('Europe/Berlin')
            .localize(datetime.combine(end_from_user, time()))
            .astimezone(pytz.timezone('UTC')))
        data_set = data_set.loc[data_set.index <= end_from_user]

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
                times.append(datetime.utcfromtimestamp(timestamp))                                
                value = float(line_tuple[1])
                data.append(value)
            
            line = file.read(9)
    
    feed = pd.DataFrame(data=data, index=times, columns=[name])
    feed.index = feed.index.tz_localize(pytz.utc)
    feed.index.name = 'timestamp'
    feed = feed.loc[feed.index.year > 1970]
    
    # Drop rows with duplicate index, as this produces problems with reindexing
    feed = feed[~feed.index.duplicated(keep='last')]
    
    return feed

