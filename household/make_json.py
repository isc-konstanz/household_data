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

description: Detailed household load and solar generation in minutely to hourly resolution

long_description: This data package contains measured time series data for several small businesses 
    and private households relevant for household- or low-voltage-level power system modeling. 
    The data includes solar power generation as well as electricity consumption (load) in a resolution 
    up to single device consumption. The starting point for the time series, as well as data quality, 
    varies between households, with gaps spanning from a few minutes to entire hours. In general, 
    data is adjusted to fit uniform, regular time intervals without changing its validity.  Except for small 
    gaps, filled using linear interpolation. The numbers are cumulative power consumption/generation over time. 
    Hence overall energy consumption/generation is retained in case of data gaps. Measurements were initially 
    conducted in 3-minute intervals, later in 1-minute intervals. Data for both measurement resolutions are 
    published separately in large CSV files. Additionally, data in 15 and 60-minute resolution is provided 
    for compatibility with other time series data. Data processing is conducted in 
    Jupyter Notebooks/Python/pandas.

documentation:
    https://github.com/isc-konstanz/household_data/blob/{version}/main.ipynb

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

contributors:
    - web: http://isc-konstanz.de/
      name: Adrian Minde
      email: adrian.minde@isc-konstanz.de

sources:
    - web: http://cossmic.eu/
      name: CoSSMic
      source: Collaborating Smart Solar-powered Microgrids - European funded research consortium
      
licenses:
    - id: CC-BY-4.0
      version: '4.0'
      name: Creative Commons Attribution-International
      url: https://creativecommons.org/licenses/by/4.0/

external: true

'''

scope_template = '{number} households in southern Germany'

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
            Region: {region}
            Type: {type}
            Household: {household}
            Feed: {feed}
'''

region_template = '''
DE_KN: Germany, Konstanz
'''

type_template = '''
residential_building_suburb: residential building, located in the suburban area
residential_building_urban: residential building, located in the urban area
residential_apartment_urban: residential apartment, located in the urban area
industrial_building_warehouse: industrial warehouse building
industrial_building_craft: industrial building of a business in the crafts sector
industrial_building_institute: industrial building, part of a research institute
school_building_urban: school building, located in the urban area
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
heating: Heating energy consumption in a {type} in {unit}
circulation_pump: Circulation pump energy consumption in a {type} in {unit}
air_conditioning: Air conditioning energy consumption in a {type} in {unit}
ventilation: Ventilation energy consumption in a {type} in {unit}
dishwasher: Dishwasher energy consumption in a {type} in {unit}
washing_machine: Washing machine energy consumption in a {type} in {unit}
refrigerator: Refrigerator energy consumption in a {type} in {unit}
freezer: Freezer energy consumption in a {type} in {unit}
cooling_aggregate: Cooling aggregate energy consumption in a {type} in {unit}
compressor: Compressor energy consumption in a {type} in {unit}
cooling_pumps: Cooling pumps energy consumption in a {type} in {unit}
facility: Energy consumption of an industrial- or research-facility in a {type} in {unit}
area: Energy consumption of an area, consisting of several smaller loads, in a {type} in {unit}
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
    regions_list = [] # list of geographical scopes and households
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
            
            region = h['region'] + '_' + h['household']
            if region not in regions_list:
                regions_list.append(region)
            
#             regions = yaml.load(region_template)
#             h['region_desc'] = regions[h['region']
            
            types = yaml.load(type_template)
            
            descriptions = yaml.load(
                descriptions_template.format(
                    type=types[h['type']], unit=h['unit']))
            try:
                feed = h['feed']
                prefix = feed.split(sep="_")[0]
                if feed in descriptions:
                    h['description'] = descriptions[feed]
                else:
                    h['description'] = descriptions[prefix]
            except KeyError:
                h['description'] = descriptions['default']
            
            field_list = field_list + field_template.format(**h)
        
        schemas_dict = schemas_dict + schemas_template.format(
            res_key=res_key, **info_cols) + field_list

    # Parse the YAML-Strings and stitch the building blocks together
    metadata = yaml.load(metadata_head.format(
        version=version, changes=changes))
    
    metadata['geographical-scope'] = scope_template.format(number=len(regions_list));
    metadata['resources'] = yaml.load(resource_list)
    metadata['schemas'] = yaml.load(schemas_dict)

    # write the metadata to disk
    datapackage_json = json.dumps(metadata, indent=4, separators=(',', ': '))
    with open('datapackage.json', 'w') as f:
        f.write(datapackage_json)

    return
