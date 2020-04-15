"""
Open Power System Data

Household Datapackage

imputation.py : fill functions for imputation of missing data.

"""
import logging
logger = logging.getLogger(__name__)

import numpy as np
import pandas as pd

from datetime import timedelta
from .tools import update_progress


def make_equidistant(household, household_data, interval):
    equidistant = pd.DataFrame()
    resolution = str(interval) + 'min'
    
    logger.info('Aggregate %s intervals for %s series', resolution, household['name'])
    feeds_columns = household_data.columns.get_level_values('feed')
    feeds_existing = len(household_data.columns)
    feeds_success = 0
    
    for feed_name in household['series'].keys():
        feed = household_data.loc[:, feeds_columns == feed_name].dropna()
        
        if(len(feed.index) != 0):
            # Find measurement outages, longer than 15 minutes
            index_delta = pd.Series(feed.index, index=feed.index)
            index_delta = (index_delta.shift(-1) - index_delta)/np.timedelta64(1, 'm')
            outage = index_delta.loc[index_delta > 15]
            
            # Extend index to have a regular frequency
            minute = feed.index[0].minute + (interval - feed.index[0].minute % interval)
            hour = feed.index[0].hour
            if(minute > 59):
                minute = 0
                hour += 1
            feed_start = feed.index[0].replace(hour=hour, minute=minute, second=0)
            
            minute = feed.index[-1].minute - (feed.index[-1].minute % interval)
            hour = feed.index[-1].hour
            if(minute > 59):
                minute = 0
                hour += 1
            feed_end = feed.index[-1].replace(hour=hour, minute=minute, second=0)
            
            feed_index = pd.date_range(start=feed_start, end=feed_end, freq=resolution)
            feed = feed.combine_first(pd.DataFrame(index=feed_index, columns=feed.columns))
            feed.index.name = 'timestamp'
            
            # Drop rows with outages longer than 15 minutes
            for i in outage.index:
                error = feed[i:i+timedelta(minutes=outage.loc[i])][1:-1]
                feed = feed.drop(error.index)
            
            # Interpolate the values between the irregular data points and drop them afterwards, 
            # to receive a regular index that is sure to be continuous, in order to later expose 
            # remaining gaps in the data.
            feed = feed.interpolate()
            feed = feed.reindex(index=feed_index)
            
            if equidistant.empty:
                equidistant = feed
            else:
                equidistant = equidistant.combine_first(feed)
            
        feeds_success += 1
        update_progress(feeds_success, feeds_existing)
    
    return equidistant


def fill_nan(df, name, headers, config_dir='conf'):
    '''
    Search for missing values in a DataFrame and optionally apply further 
    functions on each column.

    Parameters
    ----------    
    df : pandas.DataFrame
        DataFrame to inspect and possibly fill gaps
    name : str
        Name of the DataFrame to process
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    config_dir : str
         directory path where all configurations can be found

    Returns
    ----------    
    data_filled: pandas.DataFrame
        original df or df with gaps patched and marker column appended
    data_nan: pandas.DataFrame
        Contains detailed information about missing data

    '''
    data_nan = pd.DataFrame()
    data_filled = pd.DataFrame()

    df.index = df.index.tz_convert('UTC')
    col_marker = pd.Series(np.NaN, index=df.index)

    logger.info('Process %s gaps', name)

    feeds_existing = len(df.columns)
    feeds_success = 0

    # Get the frequency/length of one period of df
    one_period = df.index[1] - df.index[0]
    for col_name, col in df.iteritems():
        col = col.to_frame()

        # skip this column if it has no entries at all
        if col.empty:
            continue

        # tag all occurences of NaN in the data with True
        # (but not before first or after last actual entry)
        col['tag'] = (
            (col.index >= col.first_valid_index()) &
            (col.index <= col.last_valid_index()) &
            col.isnull().transpose().values
        ).transpose()

        # make another DF to hold info about each region
        nan_blocks = pd.DataFrame()

        # first row of consecutive region is a True preceded by a False in tags
        nan_blocks['start_idx'] = col.index[col['tag'] & ~col['tag'].shift(1).fillna(False)]

        # last row of consecutive region is a False preceded by a True
        nan_blocks['till_idx'] = col.index[col['tag'] & ~col['tag'].shift(-1).fillna(False)]

        nan_blocks = nan_blocks.sort_values('till_idx').reset_index()

        if not col['tag'].any():
            #logger.debug('Nothing to fill in for column %s', col_name_str)
            
            col.drop('tag', axis=1, inplace=True)
            nan_idx = pd.MultiIndex.from_arrays([
                [0, 0, 0, 0],
                ['count', 'span', 'start_idx', 'till_idx']])
            nan_list = pd.DataFrame(index=nan_idx, columns=col.columns)

        else:
            # how long is each region
            nan_blocks['span'] = (
                nan_blocks['till_idx'] - nan_blocks['start_idx'] + one_period)
            nan_blocks['count'] = (nan_blocks['span'] / one_period)
            
            col.drop('tag', axis=1, inplace=True)
            
            col, col_marker = _interpolate(df, name, col, col_name, col_marker, nan_blocks, one_period)
            
            nan_list = nan_blocks.copy()
            # Excel does not support datetimes with timezones, hence they need to be removed
            nan_list['start_idx'] = nan_list['start_idx'].dt.tz_convert('UTC').dt.tz_localize(None)
            nan_list['till_idx'] = nan_list['till_idx'].dt.tz_convert('UTC').dt.tz_localize(None)
            nan_list = nan_list.stack().to_frame()
            nan_list.columns = col.columns
        
        if data_filled.empty:
            data_filled = col
        else:
            data_filled = data_filled.combine_first(col)

        if data_nan.empty:
            data_nan = nan_list
        else:
            data_nan = data_nan.combine_first(nan_list)
        
        feeds_success += 1
        update_progress(feeds_success, feeds_existing)

    # append the marker to the DataFrame
    tuples = [('interpolated', '', '', '', '')]
    col_marker = col_marker.to_frame()
    col_marker.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    data_filled = pd.concat([data_filled, col_marker], axis=1)

    # set the level names for the output
    data_nan.columns.names = headers
    data_filled.columns.names = headers

    return data_filled, data_nan


def _interpolate(df, name, col, col_name, col_marker, nan_blocks, one_period):
    '''
    Choose the appropriate function for filling a region of missing values.

    Parameters
    ----------  
    col : pandas.DataFrame
        A column from frame as a separate DataFrame
    col_name : tuple
        tuple of header levels of column to inspect
    col_marker : pandas.DataFrame
        An n*1 DataFrame specifying for each row which of the previously treated 
        columns have been patched
    nan_blocks : pandas.DataFrame
        DataFrame with each row representing a region of missing data in col
    one_period : pandas.Timedelta
        Time resolution of frame and col

    Returns
    ----------  
    col : pandas.DataFrame
        An n*1 DataFrame containing col with nan_blocks filled
        and another column for the marker
    col_marker: pandas.DataFrame
        Definition as under Parameters, but now appended with markers for col 

    '''
    for i, nan_block in nan_blocks.iterrows():
        # Interpolate missing value spans up to 2 hours
        if nan_block['span'] <= timedelta(hours=1):
            col = _interpolate_hour(i, nan_block, col, one_period)
        
        elif col_name[4] == 'pv':
            col = _impute_by_day(i, nan_block, col, col_name, one_period, 1)
        else:
            col = _impute_by_day(i, nan_block, col, col_name, one_period, 7)
        
        # Create a marker column to mark where data has been interpolated
        comment_now = slice(nan_block['start_idx'], nan_block['till_idx'])
        comment_before = col_marker.notnull()
        comment_again = comment_before.loc[comment_now]
        
        col_name_str = next(iter([level for level in col_name if level in df.columns.get_level_values('region')] or []), None) + '_' + \
                        next(iter([level for level in col_name if level in df.columns.get_level_values('household')] or []), None).replace(' ', '').lower() + '_' + \
                        next(iter([level for level in col_name if level in df.columns.get_level_values('feed')] or []), None)
        
        if comment_again.any():
            col_marker[comment_before & comment_again] = col_marker + ' | ' + col_name_str
        else:
            col_marker.loc[comment_now] = col_name_str
    
    logger.debug('Interpolated %s %s gaps: %i blocks of NaN values', name, col_name[4], nan_blocks.shape[0])
    
    return col, col_marker


def _interpolate_hour(i, nan_block, col, one_period):
    '''
    Interpolate one missing value region in one column as described by 
    nan_block.

    The default pd.Series.interpolate() function does not work if
    interpolation is to be restricted to periods of a certain length.
    (A limit-argument can be specified, but it results in longer periods 
    of missing data to be filled parcially) 

    Parameters
    ----------
    i : int
        Counter for total number of regions of missing data
    nan_block : pandas.Series
        Contains information on one region of missing data in col
        count: 
        span:
        start_idx:
        till_idx:
    See _interpolate() for info on other parameters.

    Returns
    ----------
    col : pandas.DataFrame
        The column with the treated nan_block

    '''
    to_fill = slice(nan_block['start_idx'] - one_period,
                    nan_block['till_idx'] + one_period)

    col.iloc[:, 0].loc[to_fill] = col.iloc[:, 0].loc[to_fill].interpolate()

    return col


def _impute_by_day(i, nan_block, col, col_name, one_period, days):
    '''
    Impute missing value spans longer than one hour based on prior data.
    
    Parameters
    ----------
    i : int
        Counter for total number of regions of missing data
    nan_block : pandas.Series
        Contains information on one region of missing data in col
        count: 
        span:
        start_idx:
        till_idx:
    See _interpolate() for info on other parameters.

    Returns
    ----------
    col : pandas.DataFrame
        The column with the treated nan_block
    '''
    start = nan_block['start_idx']
    till = nan_block['till_idx']
    if (till - start).days > days:
        till = start + timedelta(days=days) - one_period
    
    while till <= nan_block['till_idx']:
    
        days_offset = days
        while col.iloc[:, 0].loc[start:till].isnull().values.any():
            if start-timedelta(days=days_offset) < col.iloc[:, 0].index[0]:
                if days > 1:
                    logger.debug("Problem filling %i. gap in %s %s for %i prior days. Attempting with %i prior days.", 
                                i+1, col_name[1], col_name[4], days, days-1) 
                                #start-timedelta(days=days_offset), 
                                #till-timedelta(days=days_offset))
                    days -= 1
                    days_offset = days
                    if (till - start).days > days:
                        till = start + timedelta(days=days) - one_period
                    
                    continue
                
                break
            
            elif till-timedelta(days=days_offset) > nan_block['start_idx']:
                days_offset += days
                continue
            
            #logger.debug("Filling %i. gap in %s %s with data from %s to %s", i+1, col_name[1], col_name[4],
            #             start-timedelta(days=days_offset), till-timedelta(days=days_offset))
            
            col_fill = col.iloc[:, 0].loc[start - timedelta(days=days_offset) - one_period:
                                          till - timedelta(days=days_offset) + one_period]
            
            if col_fill.isnull().values.any():
                logger.debug("Problem filling %i. gap in %s %s with data from %s to %s", i+1, col_name[1], col_name[4],
                            col_fill.index[0], col_fill.index[-1])
                days_offset += days
                continue
            
            col_fill = col_fill - col_fill.iloc[0] + col.iloc[:, 0].loc[start-one_period]
            col_fill.index += timedelta(days=days_offset)
            col_delta = col_fill.iloc[-1] - col.iloc[:, 0].loc[nan_block['till_idx']+one_period]
            
            col.iloc[:, 0].loc[start:till] = col_fill
            col.iloc[:, 0].loc[till+one_period:] += col_delta
            
            days_offset += days
        
        if till == nan_block['till_idx']:
            break;
        
        start += timedelta(days=days)
        till += timedelta(days=days)
        if till > nan_block['till_idx']:
            till = nan_block['till_idx']
    
    if col.iloc[:, 0].loc[nan_block['start_idx']:nan_block['till_idx']].isnull().values.any():
        logger.warn("Unable to fill %i. gap in %s %s from %s to %s", i+1, col_name[1], col_name[4], 
                    nan_block['start_idx'], nan_block['till_idx'])
    
    return col


def resample_markers(group):
    '''Resample marker column from 15 to 60 min

    Parameters
    ----------
    group: pd.Series
        Series of 4 succeeding quarter-hourly values from the marker column
        that have to be combined into one.

    Returns
    ----------
    aggregated_marker : str or np.nan
        If there were any markers in group: the unique values from the marker
        column group joined together in one string, np.nan otherwise

    '''

    if group.notnull().values.any():
        # unpack string of marker s into a list
        unpacked = [mark for line in group if type(line) is str
                    for mark in line.split(' | ')]  # [:-1]]
        # keep only unique values from the list
        aggregated_marker = ' | '.join(set(unpacked))  # + ' | '

    else:
        aggregated_marker = np.NaN

    return aggregated_marker

