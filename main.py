# TODO work in progress, needs further development
# TODO test that NEAD outputs are correct
#
# Purpose: Read, decode, and clean ARGOS satellite raw data for automated weather stations in Antarctica.
# Output: Writes NEAD files with the decoded data.
# For more information about the NEAD format please see https://www.doi.org/10.16904/envidat.187
#
# Contributing Authors : Rebecca Buchholz, V.Trotsiuk and Lucia de Espona, Swiss Federal Research Institute WSL
# Date Last Modified: April 12, 2022
#
#
# Example commands to run main() (make sure virtual environment is activated):
#   python
#   from main import main
#
# Then call main and pass arguments as needed.
#
# No arguments:
#   main()
#
# repeatInterval:
#   main(['-r 10'])
#
# repeatInterval and localInput:
#   main(['-r 10', '-l True'])
#


import time
import argparse
from pathlib import Path
import configparser
from datetime import datetime
import os
from dotenv import load_dotenv
from ftplib import FTP
from operator import itemgetter
import pandas

from process_argos import read_argos, decode_argos
from cleaner import ArgosCleaner

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_parser():
    parser = argparse.ArgumentParser("ArgosProcessing")
    parser.add_argument('--repeatInterval', '-r', help='Run continuously every <interval> minutes')
    parser.add_argument('--localInput', '-l', help='Any string used in this argument will load local input files '
                                                   'designated in config and skip downloading files from web')
    return parser


def read_config(config_path: str):
    config_file = Path(config_path)
    config = configparser.ConfigParser()
    config.read(config_file)
    logger.info(f' Read configuration file: {config_path}')

    if len(config.sections()) < 1:
        logger.error(' Invalid config file, missing sections')
        raise ValueError('Invalid config file, missing sections')

    return config


# Returns list of file paths to local or downloaded input data file(s)
def get_input_data(config, local_input):

    # If command line localInput argument passed (with any string) assign data_file to 'data_local' from config
    if local_input:
        # TODO test this option
        data_files = config.get('DEFAULT', 'data_local')
        logger.info(f' Skipping downloading input data, using local file(s): {data_files}')

    # Else retreive data from FTP server
    else:

        # Load and assign FTP server credentials from .env file
        load_dotenv('.env')
        ftp_host = os.getenv('FTP_HOST')
        ftp_user = os.getenv('FTP_USER')
        ftp_password = os.getenv('FTP_PASSWORD')

        # Connect to FTP server
        ftp_server = FTP(ftp_host, ftp_user, ftp_password)

        # Get a list of names of files on FTP server
        ftp_names = ftp_server.nlst()

        # Assign ftp_source_list to dictionaries of names and timestamps of FTP server files
        ftp_source_list = []
        for name in ftp_names:
            timestamp = ftp_server.voidcmd(f'MDTM {name}')[4:].strip()
            ftp_source_list.append({'name': name, 'timestamp': timestamp})

        # Sort list in descending order by timestamp
        ftp_list_sorted_desc = sorted(ftp_source_list, key=itemgetter('timestamp'), reverse=True)

        # TODO add validator that makes sure 'ftp_downloads_number' can be converted to integer
        # Assign list of 'ftp_downloads_number' (from config) recently modified files on FTP server
        ftp_downloads_number = int(config.get('DEFAULT', 'ftp_downloads_number'))
        ftp_list = ftp_list_sorted_desc[:ftp_downloads_number]

        # Assign list of file names to download
        download_list = []
        for dict_item in ftp_list:
            download_list.append(dict_item['name'])

        # Download FTP files and write to input_ftp directory
        for download in download_list:
            with open(f'input_ftp/{download}', "wb") as file:
                ftp_server.retrbinary(f'RETR {download}', file.write)

        logger.info(f' Downloaded input data from FTP server')

        # Append file directory 'input_ftp' to downloaded files, exclude files with name 'log.txt'
        # Assign downloaded file paths to data_files
        data_files = [f'input_ftp/{i}' for i in download_list if not i == 'log.txt']

    return data_files


# Removes downloaded FTP files in 'input_ftp' directory except for .gitkeep
def remove_downloaded_ftp_files():
    ftp_list = [file for file in os.listdir('input_ftp') if not file.endswith('.gitkeep')]
    for file in ftp_list:
        os.remove(os.path.join('input_ftp', file))


def process_argos_data(config, local_input=None):

    # Get input data
    data = get_input_data(config, local_input)

    # Assign frames to list of pandas dataframes produced for each file in data by calling read_argos()
    frames = []
    for file in data:
        file_dataframe = read_argos(file, nrows=None)
        frames.append(file_dataframe)

    # Remove downloaded FTP files
    # TODO test with local_input option
    if local_input is None:
        remove_downloaded_ftp_files()

    # Assign argos_dataframe to concatenated dataframes produced from individual files
    argos_dataframe = pandas.concat(frames)

    # Convert argos_dataframe from bits to numbers and assign output dataframe to data_decode
    data_decode = decode_argos(argos_dataframe, remove_duplicate=True, sort=True)

    # Convert decoded data pandas dataframe to Numpy array
    data_array = data_decode.to_numpy()

    # Clean data and write csv and json files
    stations_config_path = 'config/stations.ini'
    cleaner = ArgosCleaner(stations_config_path)

    if not cleaner:
        logger.error(f'Could not load ArgosCleaner')
        raise ValueError(f'Could not load ArgosCleaner')

    # Clean Numpy array data by applying basic filters
    # Cleaner also writes NEAD files
    cleaner.clean(data_array)

    return


def main(args=None):
    """
    Main entry point for processing ARGOS satellite transmissions.
    """

    # Access arguments passed in command line
    parser = get_parser()
    args = parser.parse_args(args)

    # Read config file
    config_path = 'config/stations.ini'
    config = read_config(config_path)

    if not config:
        logger.error(f'Not valid config file: {config_path}')
        return -1

    repeat = True
    while repeat:

        # Do not repeat if the -r argument is not present
        repeat = (args.repeatInterval is not None)

        start_time = time.time()

        logger.info(" **************************** START DATA PROCESSING ITERATION (start time: {0}) "
                    "**************************** "
                    .format(datetime.fromtimestamp(start_time)
                            .strftime('%Y-%m-%d %H:%M:%S')))

        local_input = None
        # If commandline option localInput is passed assign local_input
        if args.localInput:
            local_input = args.localInput

        # Process and clean ARGOS data, write NEAD files
        process_argos_data(config, local_input)

        # Finish data processing interation
        exec_time = int(time.time() - start_time)
        logger.info(f' FINISHED data processing iteration, that took {exec_time} seconds')

        # If repeat argument passed then set sleep interval
        if repeat:
            interval = int(args.repeatInterval) * 60
            if interval > exec_time:
                wait_time = interval - exec_time
                logger.info(f' SLEEPING {wait_time} seconds before next iteration...\n')
                time.sleep(wait_time)

    return 0


if __name__ == '__main__':
    main()
