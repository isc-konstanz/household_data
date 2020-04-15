"""
Open Power System Data

Household Datapackage

validation.py : fix possible errors and wrongly measured data.

"""
import logging
logger = logging.getLogger(__name__)

import os
import yaml
import pytz
import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from .tools import update_progress, derive_power


def validate(household, household_data, config_dir='conf', verbose=False):
    '''
    Search for measurement faults in several data series of a DataFrame and remove them

    Parameters
    ----------
    household : dict
        Configuration dictionary of the household
    household_data : pandas.DataFrame
        DataFrame to inspect and possibly fix measurement errors
    config_dir : str
         directory path where all configurations can be found
    output : boolean
        Flag, if the validated feeds should be printed as human readable CSV files

    Returns
    ----------    
    result: pandas.DataFrame
        Adjusted DataFrame with result series

    '''
    result = pd.DataFrame()
    
    logger.info('Validate %s series', household['name'])
    
    feeds_columns = household_data.columns.get_level_values('feed')
    feeds_configs = _read_adjustments(household['id'], config_dir)
    feeds_output = pd.DataFrame()
    feeds_existing = len(household_data.columns)
    feeds_success = 0
    
    for feed_name in household['series'].keys():
        feed = household_data.loc[:, feeds_columns==feed_name].dropna()
        
        #Take specific actions, depending on one-time occurrences for the specific feed
        if feed_name in feeds_configs:
            for feed_configs in feeds_configs[feed_name]:
                feed = _series_adjustment(feed_configs, feed, feed_name)
        
        # Keep only the rows where the energy values are within +3 to -3 times the standard deviation.
        error_std = np.abs(feed - feed.mean()) > 3*feed.std()
        
        if np.count_nonzero(error_std) > 0:
            logger.debug("Deleted %s %s values: %s energy values 3 times the standard deviation", 
                         household['name'], feed_name, str(np.count_nonzero(error_std)))
        
        # Keep only the rows where the energy values is increasing
        feed_fixed = feed[~error_std]
        error_inc = feed_fixed < feed_fixed.shift(1)
        
        feed_size = len(feed_fixed.index)
        for time in error_inc.replace(False, np.NaN).dropna().index:
            i = feed_fixed.index.get_loc(time)
            if i > 2 and i < feed_size:
                error_flag = None
                
                # If a rounding or transmission error results in a single value being too big,
                # fix that single data point, else flag all decreasing values
                if feed_fixed.iloc[i,0] >= feed_fixed.iloc[i-2,0] and not error_inc.iloc[i-2,0]:
                    error_inc.iloc[i,0] = False
                    error_inc.iloc[i-1,0] = True
                    
                elif all(feed_fixed.iloc[i-1,0] > feed_fixed.iloc[i:min(feed_size-1,i+10),0]):
                    error_flag = feed_fixed.index[i]
                    
                else:
                    j = i+1
                    while j < feed_size and feed_fixed.iloc[i-1,0] > feed_fixed.iloc[j,0]:
                        if error_flag is None and j-i > 10:
                            error_flag = feed_fixed.index[i]
                        
                        error_inc.iloc[j,0] = True
                        j = j+1
                
                if error_flag is not None:
                    logger.warn('Unusual behaviour at index %s for %s: %s', error_flag.strftime('%d.%m.%Y %H:%M'), household['name'], feed_name)
                
        if np.count_nonzero(error_inc) > 0:
            feed_fixed = feed_fixed[~error_inc]
            logger.debug("Deleted %s %s values: %s decreasing energy values", 
                         household['name'], feed_name, str(np.count_nonzero(error_inc)))
        
        # Notify about rows where the derived power is significantly larger than the standard deviation value
        feed_power = derive_power(feed_fixed)
        
        quantile = feed_power[feed_power > 0].quantile(.99)[0]
        error_qnt = (feed_power.abs() > 3*quantile).shift(-1).fillna(False)
        error_qnt.columns = feed_fixed.columns
        
        if np.count_nonzero(error_qnt) > 0:
            feed_fixed = feed_fixed[~error_qnt]
            logger.debug("Deleted %s %s values: %s power values 3 times .99 standard deviation", 
                         household['name'], feed_name, str(np.count_nonzero(error_qnt)))
        
        if not feed_fixed.empty:
            # Always begin with an energy value of 0
            feed_fixed -= feed_fixed.dropna().iloc[0,0]
        
        if verbose:
            os.makedirs("raw_data", exist_ok=True)
            
            error_std = error_std.replace(False, np.NaN)
            error_inc = error_inc.replace(False, np.NaN)
            error_qnt = error_qnt.replace(False, np.NaN)
            
            feed_columns = [feed_name+"_energy", feed_name+"_power", feed_name+'_error_std', feed_name+'_error_inc', feed_name+'_error_qnt']
            feed_csv = pd.concat([feed, derive_power(feed), error_std, error_inc, error_qnt], axis=1)
            feed_csv.columns = feed_columns
            feed_csv.to_csv(os.path.join("raw_data", household['id']+'_'+feed_name+'.csv'), 
                            sep=',', decimal='.', encoding='utf-8')
            
            feed_output = pd.concat([feed_fixed, derive_power(feed_fixed), error_std, error_inc, error_qnt], axis=1)
            feed_output.columns = feed_columns
            feeds_output = pd.concat([feeds_output, feed_output], axis=1)
        
        result = pd.concat([result, feed_fixed], axis=1)
        
        feeds_success += 1
        update_progress(feeds_success, feeds_existing)
    
    if verbose:
        from household.visualization import plot
        plot(feeds_output, feeds_columns, household['name']) #, days=1)
    
    return result

def _read_adjustments(household_id, config_dir):
    adjustments = {}
    adjustments_file = os.path.join(config_dir, household_id+'.d', 'series.yml')
    if os.path.isfile(adjustments_file):
        with open(adjustments_file, 'r') as f:
            adjustments_yaml = yaml.load(f.read(), Loader=yaml.FullLoader)
            adjustments = adjustments_yaml['Adjustments']
    
    return adjustments

def _series_adjustment(adjustment, feed, feed_name):
    '''
    Adjust the energy data series, to take actions against e.g. energy meter counter reset
    or changed smart meters, resulting in sudden jumps of the counter

    Parameters
    ----------
    adjustment : dict of str
         subset of adjustments necessary to be made to the feed
    feed : pandas.DataFrame
        DataFrame feed series to adjust
    feed_name : str
         Name of the feed to be adjusted

    Returns
    ----------
    fixed: pandas.DataFrame
        Adjusted DataFrame of selected feed

    '''
    
    if 'start' in adjustment:
        adj_start = pytz.timezone('UTC').localize(datetime.strptime(adjustment['start'], '%Y-%m-%d %H:%M:%S'))
        if adj_start not in feed.index:
            logger.warning("Skipping adjustment outside index for in %s at %s", feed_name, adj_start)
            return feed
    else:
        adj_start = feed.index[0]
    
    if 'end' in adjustment:
        adj_end = pytz.timezone('UTC').localize(datetime.strptime(adjustment['end'], '%Y-%m-%d %H:%M:%S'))
    else:
        adj_end = feed.index[-1]
    
    adj_type = adjustment['type']
    if adj_type == 'remove':
        # A whole time period needs to be removed due to very unstable transmission
        adj_index = feed.index.get_loc(adj_start)
        adj_delta = feed.iloc[adj_index-1] - feed.iloc[adj_index]
        feed = feed.loc[(feed.index < adj_start) | (feed.index > adj_end)]
    
    elif adj_type == 'difference':
        # Changed smart meters, resulting in a lower counter value
        adj_index = feed.index.get_loc(adj_start)
        adj_delta = feed.iloc[adj_index-1] - feed.iloc[adj_index]
        feed.loc[adj_start:adj_end] = feed.loc[adj_start:adj_end] + adj_delta
    
    elif adj_type == 'fill':
        fill_hours = int(adjustment['hours']) if 'hours' in adjustment else 24
        fill_offset = timedelta(hours=fill_hours)
        if 'from' not in adjustment or adjustment['from'] == 'before':
            fill_offset *= -1
        
        fill_data = feed.loc[adj_start + fill_offset:
                             adj_end + fill_offset]
        
        fill_data = fill_data - fill_data.iloc[0] + feed.loc[adj_start]
        fill_data.index -= fill_offset
        fill_delta = fill_data.iloc[-1] - feed.loc[adj_end]
        
        feed = pd.concat([feed, fill_data.iloc[1:-2]], axis=0).sort_index()
        feed[feed.index > adj_end] += fill_delta
    
    logger.debug("Adjusted %s values (%s) from %s to %s", feed_name, adj_type, adj_start, adj_end)
    
    return feed

