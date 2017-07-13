"""
Open Power System Data

Household Datapackage

imputation.py : fill functions for imputation of missing data.

"""

from datetime import timedelta
import logging

import numpy as np
import pandas as pd

from .tools import update_progress


logger = logging.getLogger('log')
logger.setLevel('INFO')


def make_equidistant(df, household_name, resolution, interval, start, end, feeds):
    
    equidistant = pd.DataFrame()
    
    logger.info('Aggregate %s intervals for %s - feeds', resolution, household_name)
    feeds_existing = len(df.columns)
    feeds_success = 0
    
    if start is None:
        start = df.index[0].replace(minute=df.index[0].minute + min(1, int(interval/60) - df.index[0].minute % int(interval/60)), 
                                    second=0)
    
    if end is None:
        end = df.index[-1].replace(minute=df.index[-1].minute - min(1, df.index[0].minute % int(interval/60)), 
                                   second=0)
    
    for feed_name in feeds.keys():
        feed = df.loc[(df.index >= start) & (df.index <= end), df.columns.get_level_values('feed')==feed_name].dropna()
        
        # Find measurement outages, longer than 15 minutes
        index_delta = pd.Series(feed.index, index=feed.index)
        index_delta = (index_delta.shift(-1) - index_delta)/np.timedelta64(1, 'm')
        outage = index_delta.loc[index_delta > 15]
        
        # Extend index to have a regular frequency
        feed_start = feed.index[0].replace(minute=feed.index[0].minute + (int(interval/60) - feed.index[0].minute % int(interval/60)), second=0)
        feed_end = feed.index[-1].replace(minute=feed.index[-1].minute - (feed.index[-1].minute % int(interval/60)), second=0)
        
        feed_index = pd.DatetimeIndex(start=feed_start, end=feed_end, freq=resolution)
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


def find_nan(df, headers, resolution, patch=False):
    '''
    Search for missing values in a DataFrame and optionally apply further 
    functions on each column.

    Parameters
    ----------    
    df : pandas.DataFrame
        DataFrame to inspect and possibly patch
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    resolution : str
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    patch : bool, default=False
        If False, return unaltered DataFrame,
        if True, return patched DataFrame

    Returns
    ----------    
    patched: pandas.DataFrame
        original df or df with gaps patched and marker column appended
    nan_table: pandas.DataFrame
        Contains detailed information about missing data

    '''
    nan_table = pd.DataFrame()
    patched = pd.DataFrame()

    df.index = df.index.tz_localize(None)
    marker_col = pd.Series(np.nan, index=df.index)

    logger.info('Patch feeds in %s resolution', resolution)

    feeds_existing = len(df.columns)
    feeds_success = 0

    # Get the frequency/length of one period of df
    one_period = df.index[1] - df.index[0]
    for col_name, col in df.iteritems():
        col = col.to_frame()

        col_name_str = next(iter([level for level in col_name if level in df.columns.get_level_values('household')] or []), None).lower() + '_' + \
                        next(iter([level for level in col_name if level in df.columns.get_level_values('feed')] or []), None)

        # skip this column if it has no entries at all
        if col.empty:
            continue

        # tag all occurences of NaN in the data with True
        # (but not before first or after last actual entry)
        col['tag'] = (
            (col.index >= col.first_valid_index()) &
            (col.index <= col.last_valid_index()) &
            col.isnull().transpose().as_matrix()
        ).transpose()

        # make another DF to hold info about each region
        nan_regs = pd.DataFrame()

        # first row of consecutive region is a True preceded by a False in tags
        nan_regs['start_idx'] = col.index[col['tag'] & ~
                                          col['tag'].shift(1).fillna(False)]

        # last row of consecutive region is a False preceded by a True
        nan_regs['till_idx'] = col.index[
            col['tag'] & ~
            col['tag'].shift(-1).fillna(False)]

        if not col['tag'].any():
            logger.debug('Nothing to patch for column %s', col_name_str)
            
            col.drop('tag', axis=1, inplace=True)
            nan_idx = pd.MultiIndex.from_arrays([
                [0, 0, 0, 0],
                ['count', 'span', 'start_idx', 'till_idx']])
            nan_list = pd.DataFrame(index=nan_idx, columns=col.columns)

        else:
            # how long is each region
            nan_regs['span'] = (
                nan_regs['till_idx'] - nan_regs['start_idx'] + one_period)
            nan_regs['count'] = (nan_regs['span'] / one_period)
            # sort the nan_regs DataFtame to put longest missing region on top
            nan_regs = nan_regs.sort_values(
                'count', ascending=False).reset_index(drop=True)

            col.drop('tag', axis=1, inplace=True)
            nan_list = nan_regs.stack().to_frame()
            nan_list.columns = col.columns

            if patch:
                col, marker_col = choose_fill_method(col, col_name_str, 
                                                     nan_regs, marker_col, one_period)

        if patched.empty:
            patched = col
        else:
            patched = patched.combine_first(col)

        if nan_table.empty:
            nan_table = nan_list
        else:
            nan_table = nan_table.combine_first(nan_list)

        feeds_success += 1
        update_progress(feeds_success, feeds_existing)

    # append the marker to the DataFrame
    marker_col = marker_col.to_frame()
    tuples = [('interpolated_values', '', '', '', '')]
    marker_col.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    patched = pd.concat([patched, marker_col], axis=1)

    # set the level names for the output
    nan_table.columns.names = headers
    patched.columns.names = headers

    return patched, nan_table


def choose_fill_method(col, col_name_str, nan_regs, marker_col, one_period):
    '''
    Choose the appropriate function for filling a region of missing values.

    Parameters
    ----------  
    col : pandas.DataFrame
        A column from frame as a separate DataFrame
    col_name_str : str
        Name of the series to indicate interpolated values in the comments column
    nan_regs : pandas.DataFrame
        DataFrame with each row representing a region of missing data in col
    marker_col : pandas.DataFrame
        An n*1 DataFrame specifying for each row which of the previously treated 
        columns have been patched
    one_period : pandas.Timedelta
        Time resolution of frame and col

    Returns
    ----------  
    col : pandas.DataFrame
        An n*1 DataFrame containing col with nan_regs filled
        and another column for the marker
    marker_col: pandas.DataFrame
        Definition as under Parameters, but now appended with markers for col 

    '''
    for i, nan_region in nan_regs.iterrows():
        j = 0
        # Interpolate missing value spans up to 2 hours
        if nan_region['span'] <= timedelta(hours=2):
            col, marker_col = my_interpolate(i, j, nan_region, col, col_name_str, 
                                             marker_col, nan_regs, one_period)

    return col, marker_col


def my_interpolate(i, j, nan_region, col, col_name_str, marker_col, nan_regs, one_period):
    '''
    Interpolate one missing value region in one column as described by 
    nan_region.

    The default pd.Series.interpolate() function does not work if
    interpolation is to be restricted to periods of a certain length.
    (A limit-argument can be specified, but it results in longer periods 
    of missing data to be filled parcially) 

    Parameters
    ----------
    i : int
        Counter for total number of regions of missing data
    j : int
        Counter for number regions of missing data not treated by by this
        function
    nan_region : pandas.Series
        Contains information on one region of missing data in col
        count: 
        span:
        start_idx:
        till_idx:
    See choose_fill_method() for info on other parameters.

    Returns
    ----------
    col : pandas.DataFrame
        The column with all nan_regs treated for periods shorter than 1:15.

    '''
    if i + 1 == len(nan_regs):
        logger.debug('Interpolated %s intervals below 2 hours of NaNs for column %s',
                     i + 1 - j, col_name_str)

    to_fill = slice(nan_region['start_idx'] - one_period,
                    nan_region['till_idx'] + one_period)
    comment_now = slice(nan_region['start_idx'], nan_region['till_idx'])

    col.iloc[:, 0].loc[to_fill] = col.iloc[:, 0].loc[to_fill].interpolate()

    # Create a marker column to mark where data has been interpolated
    comment_before = marker_col.notnull()
    comment_again = comment_before.loc[comment_now]
    if comment_again.any():
        marker_col[comment_before & comment_again] = marker_col + \
            ' | ' + col_name_str
    else:
        marker_col.loc[comment_now] = col_name_str

    return col, marker_col


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
        aggregated_marker = np.nan

    return aggregated_marker

