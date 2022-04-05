#
# ARGOS satellite data processing functions

import pandas
import numpy

# TODO check if logging needs to be reestablished
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def read_argos(file, nrows):
    """
    Read the Argos raw file with Pandas.
    Reshape it to the one row per transmission (from 4 columns) and return a pandas df.
    :param file: path to the Argos raw file
    :param nrows: number of rows to be read, default is 300000
    :return: a pandas dataframe, with 24 columns
    """

    # logger.info(f' Reading and processing {file}...')

    # Set up the column width, column names and the timestamp split
    columns = [(0, 6), (6, 28), (28, 39), (39, 52), (52, 65), (65, -1)]
    columns_names = ['Station', 'Timestamp', 'Column1', 'Column2', 'Column3', 'Column4']
    columns_timestamp = ['Year', 'Month', 'Day', 'Hours', 'Minutes', 'Seconds', 'Substation']

    # Assign skip_rows to 0-indexed list of line numbers that should skipped and not read into the data frame
    skip_rows = get_search_strings_line_numbers(
        file,
        ['/Invalid day of the month: {0}: begin date is posterior to the last day of the year', 'ARGOS READY']
    )

    # Read the raw file with predefined columns
    df = pandas.read_fwf(file, names=columns_names, colspecs=columns, nrows=nrows, skiprows=skip_rows,
                         converters={'Station': str, 'Timestamp': str,
                                     'Column1': int, 'Column2': int, 'Column3': int, 'Column4': int})

    # Copy the station down so the part one and part two have them
    # Remove the rows where with the satelite information as it doesn't care any additional info
    df.loc[df['Station'].notnull(), 'Station'] = df.loc[df['Station'].notnull(), 'Timestamp'].str[0:6]
    df['Station'] = df['Station'].ffill()
    df = df.dropna(subset=columns_names[2:], how='all', inplace=False)

    # For each timestamp trunsmission create a separate group which later
    # will be used to set the index to transform to wide format
    df['timestamp_group'] = 0
    df.loc[~df['Timestamp'].isnull(), 'timestamp_group'] = 1
    df['timestamp_group'] = df['timestamp_group'].cumsum()

    # Fill down the information about the timestamp
    df['Timestamp'] = df['Timestamp'].ffill()

    # Count the row for each group for the transformation
    df["timestamp_row"] = df.groupby('timestamp_group').cumcount()

    # Transform data.frame to the wide format
    df = df.set_index(columns_names[:2] + ['timestamp_group'] + ['timestamp_row']).unstack()

    # Set new names and drop index
    df.columns = [f'v_{i}' for i in range(1, 17)]
    df = df.reset_index()

    # Rearange the data to correct order (same as fortran output) and set the names again
    df[columns_timestamp] = df['Timestamp'].str.split('-|:| ', 6, expand=True)
    df = df[columns_timestamp + ['Station', 'v_1', 'v_5', 'v_9', 'v_13', 'v_2', 'v_6', 'v_10',
                                 'v_14', 'v_3', 'v_7', 'v_11', 'v_15', 'v_4', 'v_8', 'v_12', 'v_16']]
    df.columns = columns_timestamp + ['Station'] + [f'v_{i}' for i in range(1, 17)]

    return df


def decode_argos(df, remove_duplicate=True, sort=True):
    """
    Decode the output of the `read_argos` from bits to the  numbers
    :param df: a pandas dataframe as an output of `read_argos` function
    :param remove_duplicate: whether to remove duplicated rows (Can be removed later)
    :param sort: whether to sort the output (Can be sorted later)
    :return: a pandas dataframe
    """

    logger.info(f' Decoding data...')

    # Drop duplicated rows, this substantially speeds up the process
    if remove_duplicate:
        df = df.drop_duplicates(subset=['Station'] + [f'v_{i}' for i in range(1, 17)], inplace=False)

    # Convert to the numpy array
    df = df.to_numpy(dtype='float', na_value=numpy.nan)

    # Correct year
    df[:, 9] = correct_year(df[:, 0], df[:, 9])

    # Vectorise the argos function and apply it to all columns
    f_argos_bit_v = numpy.vectorize(f_argos_bit)
    df[:, 8:24] = f_argos_bit_v(df[:, 8:24])

    # Put it back to the pandas dataframe and sort
    df = pandas.DataFrame(df)
    df.columns = ['Year', 'Month', 'Day', 'Hours', 'Minutes', 'Seconds', 'Substation', 'Station'] + [f'v_{i}' for i in
                                                                                                     range(1, 17)]

    # Convert Logger ID (v_1) into the integer. The fortran code truncated the values.
    df['v_1'] = df['v_1'].astype('int')

    # This shall represent the FORTRAN formating
    # df['v_1'] = df['v_1'].apply("{:8.0f}".format)
    # df['v_2'] = df['v_2'].apply("{:8.0f}".format)
    #
    # df['v_1'] = df['v_1'].astype('int')
    # df['v_2'] = df['v_2'].astype('int')

    # Again remove duplicates. It can hapend that some still remained
    if remove_duplicate:
        df = df.drop_duplicates(subset=['Station'] + [f'v_{i}' for i in range(1, 17)], inplace=False)

    if sort:
        # df = df.sort_values(by=['Station', 'v_1', 'Year', 'Month', 'Day', 'Hours', 'Minutes', 'Seconds'],
        #                     ascending=True)
        df = df.sort_values(by=['Year', 'Month', 'Day', 'Station', 'Hours', 'Minutes', 'Seconds'],
                            ascending=True)

    return df


def correct_year(x, y):
    """
    Providing two arrays check where the year is not equal to the satelite year + 1
    :param x: vector of years from satelite transmission
    :param y: vector of years from the data
    :return: corrected vector of years
    """
    # Make sure the year is the same derived from satellite and data logger
    ind = x == y + 1
    y[ind] = x[ind]

    return y


def f_argos_bit(x):
    """
    Support function to decode each single binary variable to the standard output.
    :param x: a value to be decoded
    :return: a single real value
    """

    if x is None:
        out = None
    # Test if the value is not empty
    # Carefull python has indexing 0 comparte to R or Fortran where it is 1
    else:
        # Make a binary decoding
        # Adding an empty vector for the output
        out = 0
        ins = [0] * 16

        # Loop through each of 16 numbers and gradual decrease the value of x
        for k in range(1, 17):

            if x >= 2 ** (16 - k):

                x = x - 2 ** (16 - k)
                ins[k - 1] = 1

                if k >= 4:
                    out = out + 2 ** (16 - k)

        if ins[0] == 1:
            out = out * -1
        # if(ins[2] == 0 & ins[3] == 0) out = out
        if ins[1] == 0 and ins[2] == 1:
            out = out / 10
        if ins[1] == 1 and ins[2] == 0:
            out = out / 100
        if ins[1] == 1 and ins[2] == 1:
            out = out / 1000

    return out


def write_csv(df, file):
    """
    Write a pandas dataframe to the csv file
    :param df: a pandas data frame
    :param file: a location of the file
    :return: nothing
    """
    df.to_csv(file)


def get_search_strings_line_numbers(input_file, strings_to_search):
    """
        ASSUMPTION: Returned list of line numbers that are 0-indexed!!!!!!
        :param input_file: path to the input file (raw ARGOS satellite data)
        :param strings_to_search: list of search strings, if a row has one of these search strings then it will be
            excluded in the Argos processing
        :returns Searches for the strings_to_search in input file and
            returns 0-indexed list of line numbers containing those strings
    """

    line_number = -1
    line_number_list = []

    with open(input_file, 'r') as r:

        for line in r:

            line_number += 1

            for item in strings_to_search:

                if item in line:
                    line_number_list.append(line_number)

    return line_number_list
