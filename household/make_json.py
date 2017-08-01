"""
Open Power System Data

Household Datapackage

make:json.py : create JSON meta data for the Data Package

"""

import json
import yaml

# General metadata

metadata_head = '''
title: Household Data

name: opsd_household_data

description: Detailed household load and solar in minutely to hourly resolution

long_description: This data package contains different kinds of timeseries
    data relevant for power system modelling, namely electricity consumption 
    (load) as well as solar power generation for several small businesses and 
    private households, in a resolution up to single device consumptions.
    The timeseries become available at different points in time depending on the
    sources. The data has been downloaded from the sources, resampled and merged in
    large CSV files with minutely to hourly resolution. Most measurements were done
    initially in 3 minute intervals, to later on minutely intervals. To improve the
    datas classifyability, additional 15- and 60-minute interval files are provided.
    All data processing is conducted in python and pandas and has been documented in 
    the Jupyter notebooks linked below.

documentation:
    https://github.com/isc-konstanz/household_data/blob/master/main.ipynb

version: '{version}'

last_changes: '{changes}'

keywords:
    - Open Power System Data
    - CoSSMic
    - household data
    - time series
    - power systems
    - in-feed
    - renewables
    - solar
    - power consumption

geographical-scope: Southern Germany

contributors:
    - web: http://isc-konstanz.de/
      name: Adrian Minde
      email: adrian.minde@isc-konstanz.de

sources:
    - web: http://cossmic.eu/
      name: CoSSMic
      source: Collaborating Smart Solar-powered Microgrids - European funded research consortium
'''

resource_template = '''
- path: household_data_{res_key}_singleindex.csv
  format: csv
  mediatype: text/csv
  encoding: UTF8
  schema: {res_key}
  dialect: 
      csvddfVersion: 1.0
      delimiter: ","
      lineTerminator: "\\n" 
      header: true
  alternative_formats:
      - path: household_data_{res_key}_singleindex.csv
        stacking: Singleindex
        format: csv
      - path: household_data.xlsx
        stacking: Multiindex
        format: xlsx
      - path: household_data_{res_key}_multiindex.csv
        stacking: Multiindex
        format: csv
      - path: household_data_{res_key}_stacked.csv
        stacking: Stacked
        format: csv
'''

schemas_template = '''
{res_key}:
    primaryKey: {utc}
    missingValue: ""
    fields:
      - name: {utc}
        description: Start of timeperiod in Coordinated Universal Time
        type: datetime
        format: fmt:%Y-%m-%dT%H%M%SZ
        opsd-contentfilter: true
      - name: {cet}
        description: Start of timeperiod in Central European (Summer-) Time
        type: datetime
        format: fmt:%Y-%m-%dT%H%M%S%z
      - name: {marker}
        description: marker to indicate which columns are missing data in source data
            and has been interpolated (e.g. DE_KN_Residential1_grid_import;)
        type: string
'''

field_template = '''
      - name: {region}_{household}_{feed}
        description: {description}
        type: number (float)
        unit: {unit}
        opsd-properties: 
            Region: {region_desc}
            Type: {type}
            Household: {number}
            Feed: {feed}
'''

region_template = '''
DE_KN: Germany, Konstanz
'''

type_template = '''
residential_building_suburb: residential building, located in the suburban area
'''

descriptions_template = '''
grid_import: Energy imported from the public grid in a {type} in {unit}
grid_export: Energy exported to the public grid in a {type} in {unit}
consumption: Total household energy consumption in a {type} in {unit}
pv: Total Photovoltaic energy generation in a {type} in {unit}
ev: Electric Vehicle charging energy in a {type} in {unit}
storage_charge: Battery charging energy in a {type} in {unit}
storage_discharge: Battery discharged energy in a {type} in {unit}
heat_pump: Heat pump energy consumption in a {type} in {unit}
circulation_pump: Circulation pump energy consumption in a {type} in {unit}
dishwasher: Dishwasher energy consumption in a {type} in {unit}
washing_machine: Washing machine energy consumption in a {type} in {unit}
refrigerator: Refridgerator energy consumption in a {type} in {unit}
freezer: Freezer energy consumption in a {type} in {unit}
default: Energy in {unit}
'''

# Dataset-specific metadata

# For each dataset/outputfile, the metadata has an entry in the
# "resources" list and another in the "schemas" dictionary.
# A "schema" consits of a list of "fields", meaning the columns in the dataset.
# The first 2 fields are the timestamps (UTC and CE(S)T).
# For the other fields, we iterate over the columns
# of the MultiIndex index of the datasets to contruct the corresponding
# metadata.
# The file is constructed from different buildings blocks made up of YAML-strings
# as this makes for  more readable code.


def make_json(data_sets, info_cols, version, changes, headers):
    '''
    Create a datapackage.json file that complies with the Frictionless
    data JSON Table Schema from the information in the column-MultiIndex.

    Parameters
    ----------
    data_sets: dict of pandas.DataFrames
        A dict with the series resolution as keys and the respective
        DataFrames as values
    info_cols : dict of strings
        Names for non-data columns such as for the index, for additional 
        timestamps or the marker column
    version: str
        Version tag of the Data Package
    changes : str
        Desription of the changes from the last version to this one.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    Returns
    ----------
    None

    '''

    # list of files included in the datapackage in YAML-format
    resource_list = '''
- mediatype: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
  format: xlsx
  path: household_data.xlsx
'''
    schemas_dict = ''  # dictionary of schemas in YAML-format

    for res_key, df in data_sets.items():
        field_list = ''  # list of columns in a file in YAML-format

        # Both datasets (15min and 60min) get an antry in the resource list
        resource_list = resource_list + resource_template.format(
            res_key=res_key)

        # Create the list of of columns in a file, starting with the index
        # field
        for col in df.columns:
            if col[0] in info_cols.values():
                continue
            h = {k: v for k, v in zip(headers, col)}
            
            regions = yaml.load(region_template)
            h['region_desc'] = regions[h['region']]
            
            regions = yaml.load(region_template)
            h['number'] = ''.join(i for i in h['household'] if i.isdigit())
            
            types = yaml.load(type_template)
            h['type'] = types[h['type']]
            
            descriptions = yaml.load(
                descriptions_template.format(
                    type=h['type'], unit=h['unit']))
            try:
                h['description'] = descriptions[h['feed']]
            except KeyError:
                h['description'] = descriptions['default']
            
            field_list = field_list + field_template.format(**h)
        
        schemas_dict = schemas_dict + schemas_template.format(
            res_key=res_key, **info_cols) + field_list

    # Parse the YAML-Strings and stitch the building blocks together
    metadata = yaml.load(metadata_head.format(
        version=version, changes=changes))
    metadata['resources'] = yaml.load(resource_list)
    metadata['schemas'] = yaml.load(schemas_dict)

    # write the metadata to disk
    datapackage_json = json.dumps(metadata, indent=4, separators=(',', ': '))
    with open('datapackage.json', 'w') as f:
        f.write(datapackage_json)

    return
