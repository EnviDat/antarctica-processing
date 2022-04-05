# TODO work in progress, needs further development
#
# Purpose: Read, decode, and clean ARGOS satellite raw data for automated weather stations in Antarctica.
# Output: Writes NEAD files with the decoded data.
# TODO add more information about the NEAD format
#
# Contributing Authors : Rebecca Buchholz, V.Trotsiuk and Lucia de Espona, Swiss Federal Research Institute WSL
# Date Last Modified: April 1, 2022
#
# TODO clarify example commands
#
# Example commands to run main() (make sure virtual environment is activated):
#   python
#   from main import main
#
# Then call main and pass arguments as needed.
#
# No arguments:
#   main.main()
#
# repeatInterval:
#   main.main(['-r 10'])
#
# repeatInterval and localInput:
#   main.main(['-r 10', '-l True'])
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

# import multiprocessing

# import subprocess

# from gcnet.management.commands.importers.processor.cleaner import CleanerFactory
# from gcnet.management.commands.importers.processor.process_argos import read_argos, decode_argos
# from gcnet.management.commands.importers.processor.process_goes import decode_goes
# from gcnet.util.writer import Writer


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


def get_writer_config_dict(config_parser: configparser):
    config_dict = dict(config_parser.items('file'))
    config_dict['columns'] = dict(config_parser.items('columns'))
    return config_dict


def get_input_data(config_dict: dict, local_input):

    # If command line localInput argument passed (with any string) assign data_file to 'data_local' from config
    if local_input:
        # TODO test this option
        data_files = [config_dict['data_local']]
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
        ftp_downloads_number = int(config_dict['ftp_downloads_number'])
        ftp_list = ftp_list_sorted_desc[:ftp_downloads_number]

        # Assign list of file names to download
        download_list = []
        for dict_item in ftp_list:
            download_list.append(dict_item['name'])

        # Download FTP files and write to input_ftp directory
        for download in download_list:
            with open(f'input_ftp/{download}', "wb") as file:
                ftp_server.retrbinary(f'RETR {download}', file.write)

        # logger.info(f' Downloaded input data from FTP server and wrote file(s): {download_list}')
        logger.info(f' Downloaded input data from FTP server and wrote {ftp_downloads_number} most recent file(s)')

        # Append file directory 'input_ftp' to downloaded files, exclude files with name 'log.txt'
        data_files = [f'input_ftp/{i}' for i in download_list if not i == 'log.txt']

        return data_files

# def get_csv_import_command_list(config_parser: configparser, station_type: str):
#
#     # Load stations configuration file and assign it to stations_config
#     stations_config = config_parser
#
#     # Assign variable to contain command list
#     commands = []
#
#     # Assign variables to stations_config values and loop through each station in stations_config, create list of
#     # command strings to run csv imports for each station
#     for section in stations_config.sections():
#
#         # Check config key 'active' if data for station should be processed
#         # A value of 'True' means that station data will be processed
#         # Any other value means that station data will not be processed
#         if stations_config.get(section, 'active') == 'True' and stations_config.get(section, 'type') == station_type:
#
#             csv_input = stations_config.get(section, 'csv_input')
#             model = stations_config.get(section, 'model')
#             csv_data = stations_config.get(section, 'csv_data_dir')
#
#             # Previous version used 'import_data' management command to import data
#             # csv_temporary = stations_config.get(section, 'csv_temporary')
#             # command_string = f'python manage.py import_data -s {csv_temporary} -c gcnet/config/stations.ini ' \
#             #                  f'-i {csv_data}/{csv_input} -m {model} -f True'
#
#             # Management command 'import_csv' will be used to import processed station data stored locally
#             # into corresponding database model
#             command_string = f'python manage.py import_csv -s local -i {csv_data}/{csv_input} -a gcnet -m {model}'
#
#             commands.append(command_string)
#
#     return commands


# def execute_commands(commands_list):
#     # Iterate through commands_list and execute each command
#     for station_command in commands_list:
#         try:
#             subprocess.run(station_command, shell=True, check=True, stdout=subprocess.PIPE, universal_newlines=True)
#         except subprocess.CalledProcessError:
#             logger.error(f'Could not run command: {station_command}')
#             continue


# TODO import functions and any needed dependencies
# TODO clean up temporary downloaded files
def process_argos_data(config_dict: dict, local_input=None):

    # Get input data
    data = get_input_data(config_dict, local_input)

    # Get writer configured for the cleaner output
    # writer = Writer.new_from_dict(config_dict['writer'])

    # Assign frames to list of pandas dataframes produced for each file in data
    frames = []
    for file in data:
        file_dataframe = read_argos(file, nrows=None)
        # print(file_dataframe)
        frames.append(file_dataframe)

    # Assign argos_dataframe to concatenated dataframes produced from individual files
    argos_dataframe = pandas.concat(frames)
    # print(argos_dataframe)

    data_decode = decode_argos(argos_dataframe, remove_duplicate=True, sort=True)
    # print(data_decode)

    # data_raw = read_argos(data, nrows=None)
    # data_decode = decode_argos(data_raw, remove_duplicate=True, sort=True)

    # Decode ARGOS data
    # if station_type == 'argos':
    #     data_raw = read_argos(data, nrows=None)
    #     data_decode = decode_argos(data_raw, remove_duplicate=True, sort=True)
    #
    # # Decode GOES data
    # elif station_type == 'goes':
    #     data_decode = decode_goes(data)
    #
    # else:
    #     logger.error(f' Invalid station type: {station_type}')
    #     raise ValueError(f'Invalid station type: {station_type}')

    # TODO continue refactoring from this point
    # Convert decoded data pandas dataframe to Numpy array
    # data_array = data_decode.to_numpy()

    # Clean data and write csv and json files
    # stations_config_path = 'gcnet/config/stations.ini'
    # cleaner = CleanerFactory.get_cleaner(station_type, stations_config_path, writer)
    #
    # if not cleaner:
    #     logger.error(f'No cleaner exists for station type: {station_type}')
    #     raise ValueError(f'No cleaner exists for station type: {station_type}')

    # Clean Numpy array data by applying basic filters
    # Cleaner also writes csv and json files
    # cleaner.clean(data_array)

    return


def main(args=None):
    """
    Main entry point for processing ARGOS satellite transmissions.
    """

    # Access arguments passed in command line
    parser = get_parser()
    args = parser.parse_args(args)

    # Read config file
    config_path = 'config/config.ini'
    config = read_config(config_path)

    if not config:
        logger.error(f'Not valid config file: {config_path}')
        return -1

    # Process and clean input data, write csv and json files, import csv files data into Postgres database
    repeat = True
    while repeat:

        # Do not repeat if the -r argument is not present
        repeat = (args.repeatInterval is not None)

        start_time = time.time()

        logger.info(" **************************** START DATA PROCESSING ITERATION (start time: {0}) "
                    "**************************** "
                    .format(datetime.fromtimestamp(start_time)
                            .strftime('%Y-%m-%d %H:%M:%S')))

        # Get config_dict configured for 'argos'
        config_dict = dict(config.items('argos'))
        config_dict['writer'] = get_writer_config_dict(config)

        local_input = None
        # If commandline option localInput is passed assign local_input
        if args.localInput:
            local_input = args.localInput

        # Process ARGOS data
        process_argos_data(config_dict, local_input)

        # # Assign empty processes list
        # processes = []
        #
        # # Start process
        # for station_type in ['argos', 'goes']:
        #
        #     # Get config_dict configured for station_type
        #     config_dict = dict(config.items(station_type))
        #     config_dict['writer'] = get_writer_config_dict(config)
        #
        #     local_input = None
        #     # Assign local_input if commandline option localInput is passed
        #     if args.localInput:
        #         local_input = args.localInput
        #
        #     # Process data from each station_type concurrently using multiprocessing
        #     process = multiprocessing.Process(target=process_data, args=(station_type, config_dict, local_input))
        #     processes.append(process)
        #     process.start()
        #
        # for process in processes:
        #     process.join()

        # TODO output data in NEAD format, one NEAD file per station

        # # Write short-term csv files
        # station_array = list((config.get("file", "stations")).split(","))
        # csv_short_days = int(config.get("file", "short_term_days"))
        # csv_writer_config = get_writer_config_dict(config)
        # csv_writer = Writer.new_from_dict(csv_writer_config)
        # csv_writer.write_csv_short_term(station_array, csv_short_days)

        # # Read the stations config file
        # stations_path = 'gcnet/config/stations.ini'
        # stations_config = read_config(stations_path)
        #
        # # Check if stations_config exists
        # if not stations_config:
        #     logger.error("Non-valid config file: {0}".format(stations_path))
        #     return -1

        # Import csv files into Postgres database so that data are available for API
        # logger.info(" **************************** START DATA IMPORT ITERATION **************************** ")
        #
        # # Assign empty import_processes list
        # import_processes = []
        #
        # # Get the import commands
        # goes_commands = get_csv_import_command_list(stations_config, 'goes')
        # argos_commands = get_csv_import_command_list(stations_config, 'argos')
        #
        # # Create list with both ARGOS and GOES commands
        # import_commands = [goes_commands, argos_commands]
        #
        # # Process ARGOS and GOES import commands in parallel
        # for command_list in import_commands:
        #     process = multiprocessing.Process(target=execute_commands, args=(command_list,))
        #     import_processes.append(process)
        #     process.start()
        #
        # for process in import_processes:
        #     process.join()

        exec_time = int(time.time() - start_time)
        logger.info(f' FINISHED data processing iteration, that took {exec_time} seconds')

        if repeat:
            interval = int(args.repeatInterval) * 60
            if interval > exec_time:
                wait_time = interval - exec_time
                logger.info(f' SLEEPING {wait_time} seconds before next iteration...\n')
                time.sleep(wait_time)

    return 0


if __name__ == '__main__':
    main()
