from pathlib import Path
import os
from typing import List
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Writer(object):

    def __init__(self, csv_file_path: str, json_file_path: str,
                 new_load_flag: int, groups: List[str], short_term_days: int, columns: dict):

        self.csv_file_path = csv_file_path
        self.json_file_path = json_file_path
        self.new_load_flag = new_load_flag
        self.groups = groups
        self.short_term_days = short_term_days
        self.columns = columns

    @staticmethod
    def new_from_dict(config_dict: dict):
        return Writer(
            csv_file_path=config_dict["csv_fileloc"],
            json_file_path=config_dict["json_fileloc"],
            new_load_flag=int(config_dict["newloadflag"]),
            groups=config_dict["groups"].split(','),
            short_term_days=int(config_dict["short_term_days"]),
            columns=config_dict["columns"]
        )

    def write_csv_file(self, fid, dataset):

        if len(dataset) != 0:
            formstr = '%i,%4i,%.4f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,' \
                      '%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,' \
                      '%.2f,%.2f,%.2f,%.2f,%.2f '
            try:
                np.savetxt(fid, dataset, fmt=formstr)
                logger.info(" Successfully saved {0} entries to file: {1}".format(len(dataset[:, 1]), fid.name))
            except:
                # TODO catch specific extensions and print their info
                logger.error("Could not write CSV")
        else:
            np.savetxt(fid, dataset)

    # Function to write json files
    def write_json(self, processed_data, station_num, no_data: float):

        # Assign ds data structure to processed_data
        ds = pd.DataFrame(processed_data)

        # TODO do I need this?
        if len(ds.index) == 1:
            return pd.DataFrame()

        # Replace no_data value with Nan (outputs as null in json files)
        ds.replace(to_replace=no_data, value=np.nan, inplace=True)

        # Put header (column labels) in dictionary from columns in config, convert key to int
        col_dict = {}
        for key, val in self.columns.items():
            col_dict[key] = val
            col_dict = {int(k): v for k, v in col_dict.items()}

        # Assign ds columns to header values from config values
        ds.rename(columns=col_dict, inplace=True)

        # Drop duplicate records
        ds.drop_duplicates(inplace=True)

        # Convert StationID and Year back to int so a trailing zero does not occur (i.e. 2019.0)
        ds['StationID'] = ds['StationID'].astype(int)
        ds['Year'] = ds['Year'].astype(int)

        # ---- Format date ------
        ds['doy'] = ds['DOY_Hour'].apply(np.floor).astype('int')
        ds = ds[ds['doy'] <= 366]
        ds = ds[ds['doy'] >= 1]

        ds['hour'] = round((ds['DOY_Hour'] - ds['doy']) * 24, 0).astype('int')
        ds = ds[ds['hour'] <= 23]
        ds = ds[ds['hour'] >= 0]
        ds['hour'] = ds['hour'].astype('str')

        ds = ds[ds['Year'] <= 2100]
        # TODO ask about starting year
        ds = ds[ds['Year'] >= 1990]

        ds['doy'] = ds['doy'].astype('str').apply(lambda x: '{0:0>3}'.format(x))
        ds['hour'] = ds['hour'].apply(lambda x: '{0:0>2}'.format(x))
        ds['date'] = ds['Year'].astype('str') + '-' + ds['doy'] + '-' + ds['hour']

        # TODO ask about this section
        ds.hour.unique()
        ds.doy.unique()

        ds['timestamp_iso'] = pd.to_datetime(ds['date'], format='%Y-%j-%H')
        ds = ds.sort_values('timestamp_iso', ascending=True)

        ds['timestamp'] = ds['timestamp_iso'].astype(np.int64) // 10 ** 6

        # TODO determine if some columns should be dropped
        # ds = ds.drop(['Year', 'Doyd', 'doy', 'hour', 'date'], axis=1)

        cols_dict = {
            "temp": ['timestamp_iso', 'timestamp', 'airtemp1', 'airtemp2'],
            "rh": ['timestamp_iso', 'timestamp', 'rh1', 'rh2'],
            "rad": ['timestamp_iso', 'timestamp', 'swin', 'swout', 'netrad'],
            "sheight": ['timestamp_iso', 'timestamp', 'sh1', 'sh2'],
            "stemp": ['timestamp_iso', 'timestamp', 'SnowT1', 'SnowT2', 'SnowT3', 'SnowT4', 'SnowT5',
                      'SnowT6', 'SnowT7', 'SnowT8', 'SnowT9', 'SnowT10'],
            "ws": ['timestamp_iso', 'timestamp', 'windspeed1', 'windspeed2'],
            "wd": ['timestamp_iso', 'timestamp', 'winddir1', 'winddir2'],
            "press": ['timestamp_iso', 'timestamp', 'pressure'],
            "battvolt": ['timestamp_iso', 'timestamp', 'battvolt']
        }
        for group in self.groups:
            cols = cols_dict[group]

            # Limit data_subset to particular columns
            data_subset = ds[cols]

            # Create json
            str_station = str(station_num)
            json_path = Path(self.json_file_path + str_station + group + '.json')
            data_subset.to_json(json_path, orient='records', date_format='iso')

            # Limit data_subset_shorterm to particular columns within the days specified in config setting
            start_date = (date.today() - timedelta(days=self.short_term_days)).isoformat()
            mask = (ds['timestamp_iso'] >= start_date)
            data_subset_short_term = ds.loc[mask]
            data_json_short_term = data_subset_short_term[cols]

            # Create json for short-term data
            str_station = str(station_num)
            json_path_v = Path(self.json_file_path + str_station + group + '_v.json')
            data_json_short_term.to_json(json_path_v, orient='records', date_format='iso')

    # Function to write csv files
    def write_csv(self, processed_data, station_num, year, julian_day, date_number):

        filename = Path(self.csv_file_path + str(station_num) + '.csv')

        if self.new_load_flag == 1:
            with open(filename, 'w') as fidn:
                # overwrite any existing files because newloadflag==1 means fresh run
                self.write_csv_file(fidn, processed_data)
        else:
            try:
                fsize = os.path.getsize(filename)
                if fsize != 0:  # if station file already exists append to it
                    if len(processed_data) == 0:  # if no new data
                        # TODO look at unused variables
                        indstart = 0
                        outdat = processed_data
                    else:  # there is some data in the array
                        with open(filename, "rb") as f:
                            lines = f.readlines()
                        # TODO REview at the expected type warning
                        lastline = np.genfromtxt(lines[-1:], delimiter=',')  # read last line of existing file
                        # TODO Unused??
                        lastyr = lastline[:, 1]  # get year data
                        lastjday = lastline[:, 2] + lastline[:, 3] / 24  # calculate fractional julian day
                        # number that is ascending in time, calculate last date number of old data
                        lastdatenum = year * 1e3 + julian_day
                        # find indices of dates in new part that are before what is already in csv
                        indstart = np.argwhere(date_number <= lastdatenum)
                        if len(indstart) == 0:  # all data new
                            indstart = 0
                            outdat = processed_data
                        elif len(indstart) == len(processed_data[:, 1]):  # only dates before what is already there
                            outdat = np.array([])
                        else:  # some new data some old data
                            # begin with index after what is already in file
                            outdat = processed_data[int(np.max(indstart)) + 1:, :]
                            # write outdat to station_id.dat
                            with open(filename, 'a') as fidn:
                                self.write_csv_file(fidn, outdat)
            except:  # if filename doesn't exist create new file
                # TODO properly catch the file not found exception and return an error otherwise, print exception info
                with open(filename, 'w') as fidn:
                    self.write_csv_file(fidn, processed_data)
                logger.info(" No existing .csv file for station #{0} found. Writing new .csv".format(station_num))

    # Function to write short-term csv files
    def write_csv_short_term(self, station_array: List[str], csv_short_days: int):
        stations = station_array
        num_lines = 24 * csv_short_days  # this is csv_short_days of data assuming max transmission every hour

        today = datetime.now()
        doy_now = (today - datetime(today.year, 1, 1)).days + 1
        yearn = today.year

        # Iterate through each station and write short-term csv file
        for i in range(len(stations)):

            filename = Path(self.csv_file_path + str(stations[i]) + '.csv')

            try:
                # find if there are at least numlines in file
                with open(filename, "rb") as f:
                    lines = f.readlines()
                line_count = len(lines)
                if line_count >= num_lines:
                    # TODO look at the warning regarding expected type
                    time_range = np.genfromtxt(lines[-num_lines:], delimiter=',')  # get the lines from
                    # file without reading whole csv into memory
                    doyv = time_range[:, 2]
                    yrv = time_range[:, 1]
                    if doy_now > csv_short_days:
                        time_range = time_range[(doyv >= doy_now - csv_short_days) & (yrv == yearn), :]
                    else:
                        # TODO fix 365 to account for leap years which have 366 days
                        time_range = time_range[(  # last days in year within csv_short_days range
                                                   (doyv >= 365 - csv_short_days + doy_now)
                                                    # first days in year within csv_short_days range
                                                   | (doyv < csv_short_days + 1)
                                                   & (yrv >= yearn - 1)), :]
                else:
                    time_range = np.array([])
                new_filename = Path(self.csv_file_path + str(stations[i]) + '_v.csv')
                with open(new_filename, 'w') as fidn:
                    self.write_csv_file(fidn, time_range)

            except:
                # TODO improve exception catching
                logger.error(' Station {0} has no csv file'.format(stations[i]))
