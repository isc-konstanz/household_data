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
    result: pandas.DataFrame
        Adjusted DataFrame with result series

    '''
    result = pd.DataFrame()

    logger.info('Validate %s - feeds', household_name)

    feeds_output = pd.DataFrame()
    feeds_existing = len(df.columns)
    feeds_success = 0

    for feed_name, feed_dict in feeds.items():
        feed = df.loc[:, df.columns.get_level_values('feed')==feed_name].dropna()

        # Take specific actions, depending on one-time occurrences for the specific feed
        if 'adjustments' in feed_dict.keys():
            for adjustment in feed_dict['adjustments']:
                logger.debug("Adjust energy values at %s for %s: %s", adjustment['start'], household_name, feed_name)
                
                feed = _feed_adjustment(adjustment, feed)

        # Keep only the rows where the energy values are within +3 to -3 times the standard deviation.
        error_std = np.abs(feed - feed.mean()) > 3*feed.std()

        if np.count_nonzero(error_std) > 0:
            logger.debug("Dropped %s rows outside 3 times the standard deviation for %s: %s", str(np.count_nonzero(error_std)), household_name, feed_name)

        # Keep only the rows where the energy values is increasing
        feed_fixed = feed[~error_std]
        error_inc = feed_fixed < feed_fixed.shift(1)

        feed_size = len(feed_fixed.index)
        for time in error_inc.replace(False, np.NaN).dropna().index:
            i = feed_fixed.index.get_loc(time)
            if i > 2 and i < feed_size:
                error_flag = None
                
                # If a rounding or transmission error results in a single value being too big,
                # flag that single data point, else flag all decreasing values
                if feed_fixed.iloc[i,0] >= feed_fixed.iloc[i-2,0] and not error_inc.iloc[i-2,0]:
                    error_inc.iloc[i,0] = False
                    error_inc.iloc[i-1,0] = True
                    
                elif feed_fixed.iloc[i-1,0] > feed_fixed.iloc[i+100,0]:
                    error_flag = feed_fixed.index[i]
                    
                else:
                    j = i+1
                    while j < feed_size and feed_fixed.iloc[i-1,0] > feed_fixed.iloc[j,0]:
                        if error_flag is None and j-i > 10:
                            error_flag = feed_fixed.index[i]
                        
                        error_inc.iloc[j,0] = True
                        j = j+1
                
                if error_flag is not None:
                    logger.warn('Unusual behaviour at index %s for %s: %s', error_flag.strftime('%d.%m.%Y %H:%M'), household_name, feed_name)

        if np.count_nonzero(error_inc) > 0:
            logger.debug("Dropped %s rows with decreasing energy for %s: %s", str(np.count_nonzero(error_inc)), household_name, feed_name)

        error = error_std | error_inc
        feed_fixed = feed[~error]
        
        # Keep only the rows where the derived power is not significantly larger than the median value
        feed_power = derive_power(feed_fixed)
        
        factor = 30
        median = feed_power.replace(0, np.NaN).dropna().median()
        error_med = pd.DataFrame(feed_power.abs().gt(factor*median)).shift(-1).fillna(False)
        error_med.columns = error.columns
        
        if np.count_nonzero(error_med) > 0:
            logger.debug("Dropped %s rows with %i times larger power than the median %f for %s: %s", str(np.count_nonzero(error_med)), factor, median, household_name, feed_name)
         
        error = error | error_med
        if np.count_nonzero(error) > 0:
            feed_fixed = feed[~error.astype(bool)]

        # Always begin with an energy value of 0
        feed_fixed -= feed_fixed.dropna().iloc[0,0]
        
        feed.columns = [feed_name+"_energy"]
        feed_power.columns = [feed_name+"_power"]
        feeds_output = pd.concat([feeds_output, feed, feed_power], axis=1)
        feeds_output[feed_name+'_error_std'] = error_std.replace(False, np.NaN)
        feeds_output[feed_name+'_error_inc'] = error_inc.replace(False, np.NaN)
        feeds_output[feed_name+'_error_med'] = error_med.replace(False, np.NaN)
        
        result = pd.concat([result, feed_fixed], axis=1)

        feeds_success += 1
        update_progress(feeds_success, feeds_existing)

    result.columns.names = headers

    return result


def _feed_adjustment(adjustment, feed):
    '''
    Adjust the energy data series, to take actions against e.g. energy meter counter reset
    or changed smart meters, resulting in sudden jumps of the counter

    Parameters
    ----------
    adjustment : dict of str
         subset of adjustments necessary to be made to the feed
    feed : pandas.DataFrame
        DataFrame feed series to adjust

    Returns
    ----------
    fixed: pandas.DataFrame
        Adjusted DataFrame of selected feed

    '''
    error_start = pytz.timezone('UTC').localize(datetime.strptime(adjustment['start'], '%Y-%m-%d %H:%M:%S'))
    if 'end' in adjustment:
        error_end = pytz.timezone('UTC').localize(datetime.strptime(adjustment['end'], '%Y-%m-%d %H:%M:%S'))
    else:
        error_end = feed.index[-1]
    
    error_type = adjustment['type']
    if error_type == 'remove':
        # A whole time period needs to be removed due to very unstable transmission
        error_index = feed.index.get_loc(error_start)
        error_delta = feed.iloc[error_index-1] - feed.iloc[error_index]
        feed = feed.loc[(feed.index < error_start) | (feed.index > error_end)]
    
    elif error_type == 'difference':
        # Changed smart meters, resulting in a lower counter value
        error_index = feed.index.get_loc(error_start)
        error_delta = feed.iloc[error_index-1] - feed.iloc[error_index]
        feed.loc[error_start:error_end] = feed.loc[error_start:error_end] + error_delta

    return feed

