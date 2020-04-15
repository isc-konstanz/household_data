"""
Open Power System Data

Household Datapackage

tools.py : module independent tools

"""
import sys

import numpy as np
import pandas as pd


def update_sets(key, data, data_sets):
    '''
    Update the set of DataFrames and merge passed data.

    Parameters
    ----------
    key : str
        Key for the data set to merge, e.g. 
        
    data : pandas.DataFrame
        DataFrame with the household data to merge with the set
        
    data_sets : pandas.DataFrame
        DataFrame with the set of household data

    Returns
    ----------
    data_sets : dict of pandas.DataFrame
        Subset of household data, available for the Household

    '''
    if not key in data_sets:
        data_sets[key] = data
    elif not data.empty:
        data_sets[key] = data_sets[key].combine_first(data)


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
    text = "Progress: [{0}] {1}/{2} feeds {3}\r".format(
        "#" * block + "-" * (barLength - block), count, total, status)
    sys.stdout.write(text)
    sys.stdout.flush()

    return


def derive_power(feed):
    '''
    Derive the power from energy for a DataFrame column.

    Parameters
    ----------
    feed : pandas.DataFrame
        DataFrame with the feeds energy in the first column

    Returns
    ----------
    fixed: pandas.DataFrame
        DataFrame with the power series of the feed

    '''
    feed_energy = feed.iloc[:,0].astype('float64')
    delta_energy = feed_energy.diff()
    delta_index = pd.Series(delta_energy.index, index=delta_energy.index)
    delta_index = (delta_index - delta_index.shift(1))/np.timedelta64(1, 'h')
    
    feed_power = pd.DataFrame(delta_energy/delta_index, index=feed.index)
    feed_power.columns = ["Power [kW]"]
    
    return feed_power.dropna()

