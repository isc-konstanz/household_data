"""
Open Power System Data

Household Datapackage

visualization.py : plot energy and power values to visually validate data series.

"""
import logging
logger = logging.getLogger(__name__)

import numpy as np
import pandas as pd

import datetime as dt
from household.tools import derive_power


def visualize(data):
    for household_name in data.columns.get_level_values('household').drop_duplicates().drop(''):
        household_data = data.loc[:,(data.columns.get_level_values('household') == household_name)]
        
        feeds_data = pd.DataFrame()
        feeds_columns = household_data.columns.get_level_values('feed')
        for feed_name in feeds_columns:
            feed = household_data.loc[:,(household_data.columns.get_level_values('feed') == feed_name)].dropna()
            if feed.empty:
                continue
            
            feed.columns = [feed_name+"_energy"]
            feed_power = derive_power(feed)
            feed_power.columns = [feed_name+"_power"]
            feeds_data = pd.concat([feeds_data, feed, feed_power], axis=1)
            feeds_data.loc[:, feed_name+"_interpolated"] = data.loc[:, 'interpolated']\
                                                               .apply(lambda x: True if isinstance(x, str) and feed_name in x else np.NaN)
        
        plot(feeds_data, feeds_columns, household_name)


def plot(feeds_data, feeds_columns, household_name, days=7):
    ''' 
    Plot energy and power values to visually validate data series
    
    Parameters
    ----------
    feeds_data : pandas.DataFrame
        DataFrame to inspect and possibly fix measurement errors
    feeds_columns : dict of str
        Subset of feed columns available for the Household
    household_name : str
        Name of the Household to indicate progress
        on the households history and error susceptibility, if necessary
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    '''
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.dates import DateFormatter
    from pandas.plotting import register_matplotlib_converters
    register_matplotlib_converters()
    
    from IPython import get_ipython
    get_ipython().run_line_magic('matplotlib', 'inline')
    get_ipython().run_line_magic('matplotlib', 'qt')
    
    plt.close('all')
    plt.rcParams.update({'figure.max_open_warning': 0})
    
    colors = plt.get_cmap('tab20c').colors
    
    start = feeds_data.index[0]
    while start <= feeds_data.index[-1]:
        end = start + dt.timedelta(days=days)
        if end >= feeds_data.index[-1]:
            data = feeds_data[start:]
        else:
            data = feeds_data[start:end]
        
        errors = any('_error_' in column for column in data.columns) \
                 and data.filter(regex=("_error_")).dropna(how='all').empty
        
        if data.empty or errors:
            if data.empty:
                logger.warning('Skipping empty interval at index %s for %s', start.strftime('%d.%m.%Y %H:%M'), household_name)
            #else:
            #logger.debug('Skipping visualization for expected behaviour at index %s for %s', start.strftime('%d.%m.%Y %H:%M'), household_name)
            start = end
            continue
        
        fig, ax = plt.subplots(nrows=2)
        fig.autofmt_xdate()
        
        manager = plt.get_current_fig_manager()
        manager.window.showMaximized()
        
        legend = []
        legend_names = []
        colors_counter = 0
        for feed_name in feeds_columns:
            feed = data.filter(regex=(feed_name)).dropna(how='all')
            feed.index = feed.index.tz_convert('Europe/Berlin')
            
            feed_power = feed[feed_name+'_power'].dropna()
            if feed_power.empty:
                continue
            
            ax[0].plot(feed_power.index, feed_power, color=colors[colors_counter], label=feed_name)
            ax[0].xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M:%S'))
            ax[0].set_ylabel('Power')
            
            energy = feed[feed_name+'_energy'] - feed.loc[feed.index[0], feed_name+'_energy']
            ax[1].plot(feed.index, energy, color=colors[colors_counter]) #, linestyle='--')
            ax[1].xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M:%S'))
            ax[1].set_ylabel('Energy')
            
            if errors:
                ax[1].plot(feed.index, feed[feed_name+'_error_std'].replace(True, -1), color=colors[colors_counter], marker='o', linestyle='None')
                ax[1].plot(feed.index, feed[feed_name+'_error_inc'].replace(True, -1), color=colors[colors_counter], marker='x', linestyle='None')
                ax[0].plot(feed.index, feed[feed_name+'_error_qnt'].replace(True, -.1), color=colors[colors_counter], marker='x', linestyle='None')
            
            if any('_interpolated' in column for column in data.columns):
                ax[1].plot(feed.index, feed[feed_name+'_interpolated'].replace(True, -1), color=colors[colors_counter], marker='o', linestyle='None')
            
            legend.append(Line2D([0], [0], color=colors[colors_counter]))
            legend_names.append(feed_name)
            
            colors_counter += 2
            if colors_counter == 20:
                colors_counter = 1
        
        ax[0].legend(legend, legend_names, loc='best')
        ax[0].title.set_text(household_name)
        
        start = end
        
    plt.show()
