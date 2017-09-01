"""
Open Power System Data

Household Datapackage

tools.py : module independent tools

"""
import sys

import numpy as np
import pandas as pd


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

    index_delta = pd.Series(feed.index, index=feed.index)
    index_delta = (index_delta - index_delta.shift(1))/np.timedelta64(1, 'h')
    feed_energy = feed.iloc[:,0].astype('float64')
    
    feed_power = pd.DataFrame((feed_energy - feed_energy.shift(1))/index_delta, index=feed.index)
    feed_power.columns = ["Power [kW]"]

    return feed_power

