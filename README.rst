Antarctica-Processing
===============================

Python project that processes data from automated weather stations
in Antarctica. Station data are transmitted via the ARGOS
satellite system.

After processing data are outputted in NEAD format. One NEAD file is created for every station.
NEAD files are .csv files with an informative metadata header.
For more information about the NEAD format please see https://www.doi.org/10.16904/envidat.187


----------------------
Warning
----------------------

This project is currently under development and has not been fully tested and documented.


---------------------------------------------
Authors and Contact Information
---------------------------------------------

    * *Organization*: `Swiss Federal Research Institute WSL <https://www.wsl.ch>`_
    * *Authors*: Rebecca Buchholz, V.Trotsiuk, Lucia de Espona, Ionut Iosifescu, Derek Houtz
    * *Contact Email*: envidat(at)wsl.ch
    * *Date last modified*: April 12, 2022


------------
Installation
------------

To install antarctica-processing:

1. Clone the antarctica-processing repo from Github::

    https://github.com/EnviDat/antarctica-processing.git


2. It is recommended to create a virtual environment for this project.

   For example::

    python -m venv <path/to/project/<venv-name>


3. Activate new virtual environment::

    On macOS and Linux:
    source <venv_name>/bin/activate

    On Windows:
    .\<venv_name>\Scripts\activate


4. Install the dependencies (located in requirements.txt) into your virtual environment::

     pip install -r requirements.txt


5. Verify dependencies are installed correctly by running::

    pip list --local


--------------------------------------
.env Configuration
--------------------------------------

Create a .env file at the project root directory and enter the FTP server host,
user and password. This project assumes that a FTP server is used to host the raw
input files.

.env configuration template::

    FTP_HOST="<FTP host address>"
    FTP_USER="<FTP user>"
    FTP_PASSWORD="<FTP address>"


.env example configuration::

    FTP_HOST="ftp.wunderbar.ch"
    FTP_USER="magnificentuser"
    FTP_PASSWORD="supersecretpassword"


----------------------------------
Configuration file: stations.ini
----------------------------------

The "stations.ini" configuration file is in the directory "config".

All station-specific information and parameters should be specified in "stations.ini".
To change a calibration parameter it is only necessary to edit this file and restart "main.py" without editing the code.

**[DEFAULT] section**

The [DEFAULT] section contains the base parameters that can be overwritten in the next sections that correspond to single stations.

  * *ftp_downloads_number* is the number of most recent files to download from the FTP server.
  * *output_dir* is the directory where the output NEAD files will be written.
  * *data_local* is the path of locally stored input files. This key is only used if the input files used are local and will not be downloaded from a FTP server.
  * Other values correspond to basic filters for various scientific measurements.

Example [DEFAULT] configuration::

    [DEFAULT]
    ftp_downloads_number=336
    output_dir = output
    data_local=input/LATEST_ARGOS.raw
    swmax = 1300
    swmin = 0
    hmpmin = -40
    hmpmax = 50
    tcmax = 50
    tcmin = -100
    wmax = 50
    wmin = 0
    wdmax = 360
    wdmin = 0
    pmin = 500
    pmax = 1200
    rhmax = 130
    rhmin = 0
    shmin = -10
    shmax = 10
    battmin = 8
    battmax = 24
    active = False


**[<station ID number>] section**

Each station has its own section in stations.ini

Stations can be added and removed from stations.ini.

Example station configuration::

    [107282]
    name = Antarctica ARGOS station PE_L0
    active = True
    swin = 5.0
    swout = 5.0
    swnet_pos = 80.0
    swnet_neg = 80.0
    pressure_offset = 400

Station configuration explanation::

    [<station ID>]
    name = <station name>
    active = <if station is currently active, a value of True means data will be processed and a NEAD file will be written>
    swin = <specific calibration for station>
    swout = <specific calibration for station>
    swnet_pos = <specific calibration for station>
    swnet_neg = <specific calibration for station>
    pressure_offset = <specific calibration for station>


-----------------------------------------
Data Processing and NEAD Files Creation
-----------------------------------------

To process Argos data and write NEAD files run main.py

main.py has two optional arguments::

    -r (--repeatInterval) This runs the the import every <interval> minutes

    -l (--localInput) Any string used in this argument will load local input file designated in stations.ini config file
        and will skip downloading files from FTP server

Open terminal and navigate to project directory. Make sure virtual environment is activated.

Run python and import main::

    python
    from main import main


Then run main.py

Example commands::

    No arguments passed:                                  main()
    Repeat interval of 10 minutes:                        main.main(['-r 10'])
    Repeat interval of 10 minutes and using local input:  main.main(['-r 10', '-l True'])
