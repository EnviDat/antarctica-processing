
from pathlib import Path
import numpy as np
import configparser
from datetime import datetime
import math


import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Cleaner(object):

    def __init__(self, init_file_path: str, station_type: str):
        self.init_file_path = init_file_path
        self.stations_config = self._get_config()
        self.no_data = float(self.stations_config.get("DEFAULT", "no_data"))
        self.station_type = station_type

    def _get_config(self):
        # Set relative path to stations config file
        stations_config_file = Path(self.init_file_path)

        # Load stations configuration file and assign it to self.stations_config
        stations_config = configparser.ConfigParser()
        stations_config.read(stations_config_file)

        return stations_config

    # Function to filter values
    def _filter_values(self, unfiltered_values, sect, minimum, maximum):
        # Filter out low and high values
        array = unfiltered_values
        array[array < float(self.stations_config.get(sect, minimum))] = self.no_data
        array[array > float(self.stations_config.get(sect, maximum))] = self.no_data

        return array

    # Function to filter values with calibration factor
    def _filter_values_calibrate(self, unfiltered_values, sect, minimum, maximum, calibration,
                                 no_data_min, no_data_max):
        # Multiply values by calibration factor, filter out low and high values
        array = unfiltered_values * float(self.stations_config.get(sect, calibration))
        array[array < float(self.stations_config.get(sect, minimum))] = no_data_min
        array[array > float(self.stations_config.get(sect, maximum))] = no_data_max

        return array

        # Function to return current date number

    @staticmethod
    def _get_date_num():
        # Get current date
        today = datetime.now()
        day_of_year = (today - datetime(today.year, 1, 1)).days + 1
        current_year = today.year

        # Calculate fractional julian day
        current_julian_day = day_of_year + today.hour / 24

        current_date_num = current_year * 1e3 + current_julian_day

        return current_date_num


class ArgosCleaner(Cleaner):

    def __init__(self, init_file_path: str):
        Cleaner.__init__(self, init_file_path, 'Argos')

    # Function to process ARGOS numpy array
    def clean(self, input_data: np.ndarray):

        # Assign constant for column index in input numpy array
        INPUT_STATION_ID_COL = 7

        # Assign constants for column indices and other constants used in station_array processing
        STATION_NO_DATA1 = -8190
        STATION_NO_DATA2 = 2080
        STATION_NUM_COL = 0
        STATION_YEAR_COL = 1
        STATION_JULIAN_DAY_COL = 2
        STATION_HOUR_COL = 3
        STATION_SWIN_COL = 4
        STATION_SWOUT_COL = 5
        STATION_SWNET_COL = 6
        STATION_TC1_COL = 7
        STATION_TC2_COL = 8
        STATION_HMP1_COL = 9
        STATION_HMP2_COL = 10
        STATION_RH1_COL = 11
        STATION_RH2_COL = 12
        STATION_WS1_COL = 13
        STATION_WS2_COL = 14
        STATION_WD1_COL = 15
        STATION_WD2_COL = 16
        STATION_PRESSURE_COL = 17
        STATION_SH1_COL = 18
        STATION_SH2_COL = 19
        STATION_VOLTS_COL = 30
        STATION_S_WINMAX_COL = 31
        STATION_S_WOUTMAX_COL = 32
        STATION_S_WNETMAX_COL = 33
        STATION_TC1MAX_COL = 34
        STATION_TC2MAX_COL = 35
        STATION_TC1MIN_COL = 36
        STATION_TC2MIN_COL = 37
        STATION_WS1MAX_COL = 38
        STATION_WS2MAX_COL = 39
        STATION_WS1STD_COL = 40
        STATION_WS2STD_COL = 41
        STATION_TREF_COL = 42

        # Assign other constants
        HOURS_IN_DAY = 24
        MAX_HUMIDITY = 100
        INITIALIZER_VAL = 999

        # Iterate through each station and write json and csv file
        for section in self.stations_config.sections():

            # Assign station config variables
            is_active = self.stations_config.get(section, "active")

            # Process active Argos stations
            if is_active == 'True':

                # Assign station_id
                station_id = int(section)

                logger.info(f' Cleaning {self.station_type} Station {station_id}...')

                if input_data.size != 0:

                    # Assign station_data to data associated with each station
                    station_data = np.array(input_data[input_data[:, INPUT_STATION_ID_COL] == station_id, :])

                    if len(station_data) != 0:

                        # Assign station_array to array returns from get_station_array()
                        station_array = self.get_station_array(station_data, station_id)

                        # Filter and process station_array
                        # Assign variables used to create new array that will be used to write csv files and json files
                        if len(station_array) != 0:

                            # Assign no_data values to self.no_data
                            station_array[station_array == STATION_NO_DATA1] = self.no_data
                            station_array[station_array == STATION_NO_DATA2] = self.no_data

                            # Assign year to year data
                            year = station_array[:, STATION_YEAR_COL]

                            # Assign julian_day to julian day plus fractional julian day
                            julian_day = station_array[:, STATION_JULIAN_DAY_COL] \
                                         + station_array[:, STATION_HOUR_COL] / HOURS_IN_DAY

                            # Assign date_number to year * 1000 + julian_day
                            date_num = year * 1.e3 + julian_day

                            # Assign raw_num to number of records before duplicate filtering
                            raw_num = int(len(date_num))

                            # Find only unique timestamps and their indices from date_num
                            unique_date_num_array, unique_date_num_indices = np.unique(date_num, axis=0,
                                                                                       return_index=True)

                            # Reassign station_array to records with unique timestamps
                            station_array = station_array[unique_date_num_indices, :]

                            # Reassign year data
                            year = station_array[:, STATION_YEAR_COL]

                            # Reassign julian_day to julian day plus fractional julian day
                            julian_day = station_array[:, STATION_JULIAN_DAY_COL] \
                                         + station_array[:, STATION_HOUR_COL] / HOURS_IN_DAY

                            # Reassign date_number to year * 1000 + julian_day
                            date_num = year * 1.e3 + julian_day

                            # Log how many records removed because of duplicate time stamps
                            if len(unique_date_num_indices) < raw_num:
                                duplicate_timestamps_num = raw_num - len(unique_date_num_indices)
                                logger.info(f' Removed {duplicate_timestamps_num} entries out of'
                                            f' {raw_num} records from Station {station_id} '
                                            f'because of duplicate time tags')

                            # Assign variables used to create timestamp_iso
                            julian_dy = station_array[:, STATION_JULIAN_DAY_COL]
                            hours = station_array[:, STATION_HOUR_COL] / HOURS_IN_DAY

                            # Assign unique_timestamp_indices to indices of a sort of unique datetime values along time
                            unique_timestamp_indices = np.argsort(unique_date_num_array)

                            # Crop data array to unique times
                            station_array = station_array[unique_timestamp_indices, :]
                            julian_day = julian_day[unique_timestamp_indices]  # crop julian_day vector to unique times
                            year = year[unique_timestamp_indices]
                            date_num = date_num[unique_timestamp_indices]  # leave only unique and sorted date_nums

                            # Assign variables used for timestamp_iso creation
                            julian_dy = julian_dy[unique_timestamp_indices]
                            hours = hours[unique_timestamp_indices]

                            # Assign station_number
                            # station_number = station_array[:, STATION_NUM_COL]

                            # Assign and calibrate incoming shortwave
                            swin = self._filter_values_calibrate(station_array[:, STATION_SWIN_COL], section,
                                                                 "swmin", "swmax", "swin",
                                                                 self.no_data, self.no_data)

                            # Assign and calibrate outgoing shortwave
                            swout = self._filter_values_calibrate(station_array[:, STATION_SWOUT_COL], section,
                                                                  "swmin", "swmax", "swout",
                                                                  self.no_data, self.no_data)

                            # Assign and calibrate net shortwave, negative and positive values
                            # Different stations have different calibration coefficients according to QC code
                            swnet = INITIALIZER_VAL * np.ones(np.size(swout, 0))
                            swnet[station_array[:, STATION_SWNET_COL] >= 0] = \
                                station_array[station_array[:, STATION_SWNET_COL] >= 0, STATION_SWNET_COL] \
                                * float(self.stations_config.get(section, "swnet_pos"))
                            swnet[station_array[:, STATION_SWNET_COL] < 0] = \
                                station_array[station_array[:, STATION_SWNET_COL] < 0, STATION_SWNET_COL] \
                                * float(self.stations_config.get(section, "swnet_neg"))

                            # Filter low net shortwave
                            swnet[swnet < -float(self.stations_config.get(section, "swmax"))] = self.no_data

                            # Filter high net shortwave
                            swnet[swnet > float(self.stations_config.get(section, "swmax"))] = self.no_data

                            # Filter thermocouple 1
                            tc1 = self._filter_values(station_array[:, STATION_TC1_COL], section, "tcmin", "tcmax")

                            # Filter thermocouple 2
                            tc2 = self._filter_values(station_array[:, STATION_TC2_COL], section, "tcmin", "tcmax")

                            # Filter hmp1 temp
                            hmp1 = self._filter_values(station_array[:, STATION_HMP1_COL], section, "hmpmin", "hmpmax")

                            # Filter hmp2 temp
                            hmp2 = self._filter_values(station_array[:, STATION_HMP2_COL], section, "hmpmin", "hmpmax")

                            # Assign and calibrate relative humidity 1
                            rh1 = station_array[:, STATION_RH1_COL]
                            rh1[rh1 < float(self.stations_config.get(section, "rhmin"))] = self.no_data  # filter low
                            rh1[rh1 > float(self.stations_config.get(section, "rhmax"))] = self.no_data  # filter high
                            # Assign values greater than MAX_HUMIDITY and less than rhmax to MAX_HUMIDITY
                            rh1[(rh1 > MAX_HUMIDITY) & (rh1 < float(self.stations_config.get(section, "rhmax")))] \
                                = MAX_HUMIDITY

                            # Assign and calibrate relative humidity 2
                            rh2 = station_array[:, STATION_RH2_COL]
                            rh2[rh2 < float(self.stations_config.get(section, "rhmin"))] = self.no_data  # filter low
                            rh2[rh2 > float(self.stations_config.get(section, "rhmax"))] = self.no_data  # filter high
                            # Assign values greater than MAX_HUMIDITY and less than rhmax to MAX_HUMIDITY
                            rh2[(rh2 > MAX_HUMIDITY) & (
                                    rh2 < float(self.stations_config.get(section, "rhmax")))] = MAX_HUMIDITY

                            # Filter wind speed 1
                            ws1 = self._filter_values(station_array[:, STATION_WS1_COL], section, "wmin", "wmax")

                            # Filter wind speed 2
                            ws2 = self._filter_values(station_array[:, STATION_WS2_COL], section, "wmin", "wmax")

                            # Filter wind direction 1
                            wd1 = self._filter_values(station_array[:, STATION_WD1_COL], section, "wdmin", "wdmax")

                            # Filter wind direction 2
                            wd2 = self._filter_values(station_array[:, STATION_WD2_COL], section, "wdmin", "wdmax")

                            # Assign and calibrate barometric pressure
                            pres = station_array[:, STATION_PRESSURE_COL] \
                                   + float(self.stations_config.get(section, "pressure_offset"))
                            pres[pres < float(self.stations_config.get(section, "pmin"))] = self.no_data  # filter low
                            pres[pres > float(self.stations_config.get(section, "pmax"))] = self.no_data  # filter low
                            pres_diff = np.diff(pres)  # Find difference of subsequent pressure measurements
                            hr_diff = np.diff(julian_day) * 24.  # Time difference in hours
                            mb_per_hr = np.absolute(
                                np.divide(pres_diff, hr_diff, out=np.zeros_like(pres_diff), where=hr_diff != 0)
                            )
                            press_jumps = np.argwhere(mb_per_hr > 10)  # Find jumps > 10mb/hr (quite unnatural)
                            pres[press_jumps + 1] = self.no_data  # Eliminate these single point jumps

                            # Filter height above snow 1
                            sh1 = self._filter_values(station_array[:, STATION_SH1_COL], section, "shmin", "shmax")

                            # Filter height above snow 2
                            sh2 = self._filter_values(station_array[:, STATION_SH2_COL], section, "shmin", "shmax")

                            # 10m snow temperature (many of these are non functional or not connected)
                            snow_temp10 = station_array[:, 20:30]

                            # Filter battery voltage
                            volts = self._filter_values(station_array[:, STATION_VOLTS_COL], section,
                                                        "battmin", "battmax")

                            s_winmax = self._filter_values_calibrate(station_array[:, STATION_S_WINMAX_COL], section,
                                                                     "swmin", "swmax", "swin",
                                                                     self.no_data, self.no_data)

                            s_woutmax = self._filter_values_calibrate(station_array[:, STATION_S_WOUTMAX_COL], section,
                                                                      "swmin", "swmax", "swout", 0.00, self.no_data)

                            # Assign and calibrate net radiation max
                            s_wnetmax = INITIALIZER_VAL * np.ones_like(s_woutmax)
                            s_wnetmax[station_array[:, STATION_S_WNETMAX_COL] >= 0] \
                                = station_array[station_array[:, STATION_S_WNETMAX_COL] >= 0, STATION_S_WNETMAX_COL] \
                                  * float(self.stations_config.get(section, "swnet_pos"))
                            s_wnetmax[station_array[:, STATION_S_WNETMAX_COL] < 0] \
                                = station_array[station_array[:, STATION_S_WNETMAX_COL] < 0, STATION_S_WNETMAX_COL] \
                                  * float(self.stations_config.get(section, "swnet_neg"))
                            # Filter low
                            s_wnetmax[s_wnetmax < -(float(self.stations_config.get(section, "swmax")))] = self.no_data
                            # Filter high
                            s_wnetmax[s_wnetmax > float(self.stations_config.get(section, "swmax"))] = self.no_data

                            # Filter other measurements
                            tc1max = self._filter_values(station_array[:, STATION_TC1MAX_COL], section,
                                                         "tcmin", "tcmax")

                            tc2max = self._filter_values(station_array[:, STATION_TC2MAX_COL], section,
                                                         "tcmin", "tcmax")

                            tc1min = self._filter_values(station_array[:, STATION_TC1MIN_COL], section,
                                                         "tcmin", "tcmax")

                            tc2min = self._filter_values(station_array[:, STATION_TC2MIN_COL], section,
                                                         "tcmin", "tcmax")

                            # Assign statistics
                            ws1max = station_array[:, STATION_WS1MAX_COL]
                            ws2max = station_array[:, STATION_WS2MAX_COL]
                            ws1std = station_array[:, STATION_WS1STD_COL]
                            ws2std = station_array[:, STATION_WS2STD_COL]
                            tref = station_array[:, STATION_TREF_COL]

                            # Assemble filtered data into data_filtered 2d array
                            data_filtered = np.column_stack(
                                (swin, swout, swnet, tc1, tc2, hmp1, hmp2, rh1, rh2,
                                 ws1, ws2, wd1, wd2, pres, sh1, sh2, volts, s_winmax, s_woutmax,
                                 s_wnetmax, tc1max, tc2max, tc1min, tc2min, ws1max, ws2max, ws1std, ws2std, tref)
                            )

                            # Create 1d array of timestamp_iso datetime objects from existing time data
                            timestamp_iso = self.get_timestamp_iso(year, julian_dy, hours)

                            # Combine timestamp_iso and data_filtered arrays into timestamped_data 2d array
                            timestamped_data = np.column_stack((timestamp_iso, data_filtered))

                            # If nead_header exists write NEAD file with cleaned data
                            nead_header = self.get_nead_header(station_id)
                            if nead_header is not None:
                                # nead_header_config = configparser.ConfigParser()
                                # nead_header_config.read(Path(f'output/{str(station_id)}_NEAD.csv')
                                self.write_nead(timestamped_data, station_id, nead_header)

                        # Else station_array is empty after removing bad dates
                        else:
                            logger.warning(f'\t{self.station_type} Station {station_id} does not have usable data')

                else:
                    logger.warning(f'\t{self.station_type} Station {station_id} does not have usable data')

    # Writes NEAD file for cleaned station data
    @staticmethod
    def write_nead(cleaned_data, station_id, nead_header):

        # TODO pass csv_file_path from config
        filename = Path(f'output/{str(station_id)}_NEAD.csv')

        with open(filename, 'w') as file:
            if len(cleaned_data) != 0:
                # Create format_string from number of columns of cleaned_data
                cleaned_data_columns_num = cleaned_data.shape[1]
                format_string = '%s,'*cleaned_data_columns_num
                try:
                    np.savetxt(file, cleaned_data, fmt=format_string, header=nead_header)
                    logger.info(" Wrote {0} entries for Station {1} to file: {2}"
                                .format(len(cleaned_data[:, 1]), station_id, filename))
                except Exception as e:
                    logger.error(f' ERROR COULD NOT WRITE CSV, EXCEPTION: {e}')
            # TODO test with no data
            else:
                np.savetxt(file, cleaned_data)

    # Returns NEAD header as a string if it exists, else returns None
    @staticmethod
    def get_nead_header(station_id):

        nead_header_path = Path(f'nead_config/{station_id}.ini')

        if nead_header_path.is_file():
            with open(nead_header_path, 'r') as file:
                nead_header = file.read()
            return nead_header

        else:
            logger.error(f' ERROR CAN NOT WRITE NEAD FILE FOR STATION {station_id}: {nead_header_path} does not exist')
            return None

    # Returns timestamp in ISO UTC format, for example '2020-11-03 00:00:00+00:00'
    # Returns unix timestamp
    @staticmethod
    # TODO make timezone configurable
    def get_timestamp_iso(year, julian_day, hours, timezone='+0000'):

        year = year.astype(int).astype(str)
        julian_day = julian_day.astype(int).astype(str)
        hours = (hours * 24).astype(int).astype(str)

        # Combine year, julian_day, hours into timestamps
        timestamps = np.stack((year, julian_day, hours), axis=1)

        # Assign timestamps_formatted to list of timestamps in ISO format
        timestamps_formatted = []
        for index in range(len(timestamps)):

            timestamp = f'{timestamps[index][0]}-' \
                        f'{(timestamps[index][1]).zfill(3)}-' \
                        f'{(timestamps[index][2]).zfill(2)} ' \
                        f'{timezone}'

            # Convert timestamp string to datetime object and append to timestamps_formatted
            dt_object = datetime.strptime(timestamp, '%Y-%j-%H %z')
            timestamps_formatted.append(dt_object)

        # Convert timestamps_formattted into Numpy 1d array timestamps_iso
        timestamps_iso = np.array(timestamps_formatted)

        return timestamps_iso


    # Returns station_array which is the array for the data from each station
    # created from the combined first and second parts of the input table
    @staticmethod
    def get_station_array(station_data, station_id):

        # Assign constants for column indices in input numpy array
        INPUT_YEAR1_COL = 0
        INPUT_STATION_NUM_COL = 8
        INPUT_YEAR2_COL = 9
        INPUT_JULIAN_DAY_COL = 10
        INPUT_WIND_DIRECTION_COL = 9

        # Assign constants for column indices and other constants in combined_array
        COMBINED_YEAR_COL = 1
        COMBINED_YEAR_MIN = 1990
        COMBINED_YEAR_MAX = 2050
        COMBINED_JULIAN_DAY_COL = 2

        # Assign other constants
        MAX_DAYS_YEAR = 367
        MAX_DEGREES_WIND = 360
        INITIALIZER_VAL = 999

        # Assign unique_array to unique_rows after INPUT_STATION_NUM_COL
        # Assign unique_indices to indices of unique rows after INPUT_STATION_NUM_COL
        # because data may repeat with different time signature
        unique_array, unique_indices = np.unique(station_data[:, INPUT_STATION_NUM_COL:],
                                                 axis=0, return_index=True)

        # Assign station_data to station_data sorted by unique_indcies
        station_data = station_data[np.sort(unique_indices), :]

        # Assign table_1_indices to indices of rows that are the first part of the two part table
        # and have integer Julian day (records with decimal julian day are erroneous)
        # and have a realistic Julian day (positive and less than 367 day, leap year will have 366 days)
        table_1_indices = np.argwhere(
            (station_data[:, INPUT_YEAR1_COL] == station_data[:, INPUT_YEAR2_COL]) &
            (np.ceil(station_data[:, INPUT_JULIAN_DAY_COL]) ==
             np.floor(station_data[:, INPUT_JULIAN_DAY_COL])) &
            (station_data[:, INPUT_JULIAN_DAY_COL] > 0) &
            (station_data[:, INPUT_JULIAN_DAY_COL] < MAX_DAYS_YEAR))

        # Assign table_2_indices to indices of rows that are the second part of the two part table
        # column 9 of 2nd table is wind direction, realistic values will be less than 360 degrees
        table_2_indices = np.argwhere(
            (station_data[:, INPUT_YEAR1_COL] != station_data[:, INPUT_WIND_DIRECTION_COL]) &
            (station_data[:, INPUT_WIND_DIRECTION_COL] <= MAX_DEGREES_WIND))

        # Assign table_2_indices_last_item to last item in table_2_indices
        table_2_indices_last_item = table_2_indices[-1:]

        # Make sure last record in table 1 has a second piece of the table
        table_1_indices = table_1_indices[table_1_indices < table_2_indices_last_item]

        # Assign num_records to length of table_1_indices
        num_records = len(table_1_indices)

        # Assign combined_array as an array that will be used to
        # combine data from table 1 and table 2, inialize all values as INITIALIZER_VAL
        combined_array = np.ones((num_records, 43)) * INITIALIZER_VAL

        # Assign combined_array_columns to columns to be used in combined_array
        combined_array_columns = np.concatenate(
            (np.arange(0, 20), np.arange(30, 33), np.arange(34, 38), np.array([38]), np.array([39])))

        # Assign table_1_columns to columns in table 1 raw
        table_1_columns = np.concatenate(
            (np.array([0]), np.array([10]), np.array([3]), np.arange(12, 23)))

        # Assign table_2_columns to columns in table 2 raw
        table_2_columns = np.concatenate((np.arange(9, 14), np.array([22]), np.arange(14, 22)))

        # Loop through records
        for j in range(num_records):
            # Find second table parts occurring after associated first part
            table_2_current_indices = np.argwhere(
                station_data[table_1_indices[j]:, 0] != station_data[table_1_indices[j]:, 9])

            table_1_index = table_1_indices[j]

            # Assign table_2_index to the closest table 2 line
            table_2_index = table_1_indices[j] + table_2_current_indices[INPUT_YEAR1_COL]

            # Combine corresponding parts of table 1 and table 2 into an array within combined_array
            combined_array[j, combined_array_columns] = np.concatenate(
                (np.array([station_id]),
                 station_data[table_1_index, table_1_columns],
                 station_data[table_2_index, table_2_columns]))

        # Assign station_array to combined_array filtered for realistic years and Julian days
        station_array = combined_array[(combined_array[:, COMBINED_YEAR_COL] > COMBINED_YEAR_MIN) &
                                       (combined_array[:, COMBINED_YEAR_COL] < COMBINED_YEAR_MAX) &
                                       (combined_array[:, COMBINED_JULIAN_DAY_COL] >= 0) &
                                       (combined_array[:, COMBINED_JULIAN_DAY_COL] < MAX_DAYS_YEAR), :]

        return station_array
