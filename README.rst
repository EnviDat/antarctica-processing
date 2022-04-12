Antarctica-Processing
===============================

Python project that processes data from automated weather stations
in Antarctica. Station data are transmitted via the ARGOS
satellite system.
After processing data are outputted in NEAD format.

----------------------
Warning
----------------------

This project is currently under development and has not been fully tested.

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

.env Configuration Template::

    FTP_HOST="<FTP host address>"
    FTP_USER="<FTP user>"
    FTP_PASSWORD="<FTP address>"


.env Example Configuration::

    FTP_HOST="ftp.wunderbar.ch"
    FTP_USER="magnificentuser"
    FTP_PASSWORD="supersecretpassword"



