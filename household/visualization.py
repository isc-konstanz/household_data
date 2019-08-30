"""
Open Power System Data

Household Datapackage

visualization.py : plot energy and power values to visually validate data series.

"""
import pandas as pd

from .tools import derive_power

def visualize(df):
    
    for household_name in df.columns.get_level_values('household').drop_duplicates().drop(''):
        household_data = df.loc[:,(df.columns.get_level_values('household') == household_name)]
        
        feeds_output = pd.DataFrame()
        feed_columns = household_data.columns.get_level_values('feed')
        for feed_name in feed_columns:
            feed = household_data.loc[:,(household_data.columns.get_level_values('feed') == feed_name)]
            if feed.empty:
                continue
            
            feed.columns = [feed_name+"_energy"]
            feed_power = derive_power(feed)
            feed_power.columns = [feed_name+"_power"]
            feeds_output = pd.concat([feeds_output, feed, feed_power], axis=1)
        
        _plot(feeds_output, feed_columns, household_name)


def _plot(df, feed_columns, household_name):
    ''' 
    Plot energy and power values to visually validate data series
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to inspect and possibly fix measurement errors
    feed_columns : dict of int
        Subset of feed ids, available for the Household
    household_name : str
        Name of the Household to indicate progress
        on the households history and error susceptibility, if necessary
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    '''
   
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from pandas.plotting import register_matplotlib_converters
    register_matplotlib_converters()
    from IPython import get_ipython
    get_ipython().run_line_magic('matplotlib', 'inline')
    get_ipython().run_line_magic('matplotlib', 'qt')
    
    plt.close('all')
    plt.rcParams.update({'figure.max_open_warning': 0})
    colors = plt.get_cmap('tab20').colors
    
    start = df.index[0]
    while start <= df.index[-1]:
        end = start + pd.Timedelta(weeks=1)
        if end >= df.index[-1]:
            data = df[start:]
        else:
            data = df[start:end]
        
        if data.empty or ('_error_' in data.columns and data.filter(regex=("_error_")).dropna(how='all').empty):
            start = end
            continue
        
        _, ax1 = plt.subplots()
        plt.title(household_name)
        
        manager = plt.get_current_fig_manager()
        manager.window.showMaximized()
        ax2 = ax1.twinx()
        legend = []
        legend_names = []
        colors_counter = 0
        for feed_name in feed_columns:
            feed = data.filter(regex=(feed_name)).dropna(how='all')
            feed.index = feed.index.tz_convert('Europe/Berlin')
            if feed.empty:
                continue
            
            power = feed[feed_name+'_power'].dropna()
            ax1.plot(power.index, power, color=colors[colors_counter], label=feed_name)
            ax1.set_ylabel('Power')
            energy = feed[feed_name+'_energy'] - feed.loc[feed.index[0], feed_name+'_energy']
            ax2.set_ylabel('Energy')
            ax2.plot(energy.index, energy, color=colors[colors_counter], linestyle='--')
            
            if '_error_' in feed.columns:
                ax1.plot(feed.index, feed[feed_name+'_error_std'].replace(True, -0.1), color=colors[colors_counter], marker='o', linestyle='None')
                ax1.plot(feed.index, feed[feed_name+'_error_inc'].replace(True, -0.1), color=colors[colors_counter], marker='x', linestyle='None')
                ax1.plot(feed.index, feed[feed_name+'_error_med'].replace(True, -0.1), color=colors[colors_counter], marker='^', linestyle='None')
            
            legend.append(Line2D([0], [0], color=colors[colors_counter]))
            legend_names.append(feed_name)
            ax1.legend(legend, legend_names,loc= 'best')
            
            colors_counter += 2
            if colors_counter == 20:
                colors_counter = 1
            
        start = end

