Intelica Interchange System
========================
The system is in charge of identifying, processing and uploading customer files based on transactions of Visa and Mastercard and what is related to them. Since it is a transaction scheme, support processes are required, in this case the extraction of type of change to obtain the respective validations. To see the details of the modules that are executable, you can access the README.md in each module to consult.


## Installation
System requires [Python3.10](https://www.python.org/downloads/release/python-3100/) to run

1. Install the dependencies and devDependencies to start to execute.


2. By default, everything is already in the environment where to activate it the following line is required:

```
source ~/interchange/interchange_env/bin/activate
```

How to use the application
--------------------
It happens to execute the process in general, part of the interpretation of the zip/rar that includes the files to be processed. This is done through the following command for example:

```
python main.py interpretation -c BRDRO -f 11_0ct.zip
```
