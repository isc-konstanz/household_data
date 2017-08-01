"""
Open Power System Data

Household Datapackage

validation.py : fix possible errors and wrongly measured data.

"""
from datetime import datetime
import logging

import pytz

import numpy as np
import pandas as pd

from .tools import update_progress, derive_power


logger = logging.getLogger('log')
logger.setLevel('INFO')


def validate(df, household_name, feeds, headers, output=False):
    '''
    Search for measurement faults in several data series of a DataFrame and remove them

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to inspect and possibly fix measurement errors
    household_name : str
        Name of the Household to indicate progress
        on the households history and error susceptibility, if necessary
    feeds : dict of int
        Subset of feed ids, available for the Household
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    output : boolean
        Flag, if the validated feeds should be printed as human readable CSV files

    Returns
    ----------    
    fixed: pandas.DataFrame
        Adjusted DataFrame with fixed series

    '''
    fixed = pd.DataFrame()

    logger.info('Validate %s - feeds', household_name)

    feeds_existing = len(df.columns)
    feeds_success = 0

    for feed_name, feed_dict in feeds.items():
        feed = df.loc[:, df.columns.get_level_values('feed')==feed_name].dropna()

        # Take specific actions, depending on one-time occurrences for the specific feed
        if 'adjustments' in feed_dict.keys():
            for time in feed_dict['adjustments']:
                logger.debug("Adjust energy counter value at %s for %s: %s", time, household_name, feed_name)
                
                feed = _feed_adjustment(time, feed)

        # Keep only the rows where the energy values are within +3 to -3 standard deviation.
        error_std = np.abs(feed - feed.mean()) > 3*feed.std()

        if np.count_nonzero(error_std) > 0:
            logger.debug("Dropped %s rows outside 3 times the standard deviation for %s: %s", str(np.count_nonzero(error_std)), household_name, feed_name)

        # Keep only the rows where the energy values is increasing
        error_inc = feed[~error_std] < feed[~error_std].shift(1)

        feed_size = len(feed.index)
        for time in error_inc.replace(False, np.NaN).dropna().index:
            i = feed.index.get_loc(time)
            if i > 2 and i < feed_size:
                # If a rounding or transmission error results in a single value being too big,
                # flag that single data point, else flag all decreasing values
                if feed.iloc[i,0] >= feed.iloc[i-2,0] and not error_inc.iloc[i-2,0]:
                        error_inc.iloc[i,0] = False
                        error_inc.iloc[i-1,0] = True
                else:
                    error_flag = None
                    
                    j = i+1
                    while j < feed_size and feed.iloc[i-1,0] > feed.iloc[j,0]:
                        if error_flag is None and j-i > 10:
                            error_flag = feed.index[j-10]
                        
                        error_inc.iloc[j,0] = True
                        j = j+1
                    
                    if error_flag is not None:
                        logger.debug('Unusual behaviour at index %s for %s: %s', error_flag.strftime('%d.%m.%Y %H:%M'), household_name, feed_name)

        if np.count_nonzero(error_inc) > 0:
            logger.debug("Dropped %s rows with decreasing energy for %s: %s", str(np.count_nonzero(error_inc)), household_name, feed_name)

        error = error_std | error_inc
        if output:
            feed_csv = derive_power(feed)
            feed_csv[feed_name+'_error'] = error.replace(False, np.NaN)
            feed_csv.to_csv(household_name.lower()+'_'+feed_name+'.csv', sep=';', decimal=',', encoding='utf-8')

        if np.count_nonzero(error) > 0:
            feed = feed[~error]

        feed -= feed.dropna().iloc[0,0]

        if fixed.empty:
            fixed = feed
        else:
            fixed = fixed.combine_first(feed)

        feeds_success += 1
        update_progress(feeds_success, feeds_existing)

    fixed.columns.names = headers

    return fixed


def _feed_adjustment(time, feed):
    '''
    Adjust the energy data series, to take actions against e.g. energy meter counter reset
    or changed smart meters, resulting in sudden jumps of the counter

    Parameters
    ----------
    time : str
        datetime string, indicating the adjustments occurrence
    feed : pandas.DataFrame
        DataFrame feed series to adjust

    Returns
    ----------
    fixed: pandas.DataFrame
        Adjusted DataFrame of selected feed

    '''
    # Changed smart meters, resulting in a lower counter value
    error_time = pytz.timezone('UTC').localize(datetime.strptime(time, '%Y-%m-%d %H:%M:%S'))
    error_index = feed.index.get_loc(error_time)
    error_delta = feed.iloc[error_index-1] - feed.iloc[error_index]
    feed.loc[feed.index >= error_time] = feed.loc[feed.index >= error_time] + error_delta

    return feed
