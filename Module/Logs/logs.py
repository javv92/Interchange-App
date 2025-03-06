import logging
import datetime
import os
import pathlib
from typing import Any
import Module.Persistence.connection as conec
import sys
from sys import exc_info
from traceback import format_exception

class logs:
    """Class for logging all process
    
    Params:
        log_name: log file name.
    
    """
    def __init__(self,log_name = None) -> None:
        self.logger = logging.getLogger()
        fecha = datetime.datetime.now()
        self.fechaArch = fecha.strftime("%d%m%Y%H%M%S")
        self.fechainfo = fecha.strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]
        self.day = fecha.strftime("%d/%m/%Y")
        self.hour = fecha.strftime("%H:%M:%S.%f")[:-3]
        self.S3 = conec.connect_to_s3()
        self.db_conn = conec.connect_to_postgreSQL()
        self.path = "Log"
        self.log_bucket = os.getenv("LOG_BUCKET")
        self.log_name = log_name
        pass

    def write_file(self, pathfile: str, message: str)->list:
        """Create the log file on a sent path
        
        Args: 
            pathfile (str): path to file.
            message (str): message line to insert into log.

        Returns:
            list: message and condition.
        """
        try:
            file = open(pathfile, "w")
            file.write(message + "\n")
            file.close()
            return [(True, "Successed write")]
        except Exception as e:
            error = self.print_except()
            print(error)
            return [(False,error)]

    def new_log(
        self, typeLog: str, master: str, client: str, process: str, brand: str, module:str
    ) -> Any:
        """Creates a new log
        
        Args:
            typeLog (str): type of log.
            master (str): master indicator.
            client (str): client code.
            process (str): process step.
            brand (str): processed brand.
            module (str): processed module

        Returns:
            result (str): name of log
        """
        try:
            self.S3.client = client
            self.S3.exec_module = module
            self.S3.typeLog = typeLog
            error_message = (
                "Please the validate corrects parameters for create a log file."
            )
            if typeLog == "OPERATIONAL":
                message_log = (
                    self.day
                    + ";"
                    + self.hour
                    + "; "
                    + client
                    + "; "
                    + brand
                    + "; "
                    + process
                    + "; "
                    + module
                    + "; INFO; BUILT FOR FILE GETTING MODULE INTERCHANGE;"
                )
            elif typeLog == "EXCHANGE_RATE":
                message_log = (
                    self.day
                    + ";"
                    + self.hour
                    + "; "
                    + "INTELICA"
                    + "; "
                    + brand
                    + "; "
                    + "GET EXCHANGE RATE VALUE"
                    + "; "
                    + module
                    + "; INFO; BUILT FOR FILE GETTING MODULE EXCHANGE_RATE;"
                )
            elif typeLog == "MASTER":
                message_log = (
                    self.day
                    + ";"
                    + self.hour
                    + "; "
                    + "INTELICA"
                    + "; "
                    + brand
                    + "; "
                    + master
                    + "; "
                    + module
                    + "; INFO; BUILT FOR FILE GETTING MODULE MASTER;"
                )
            else:
                print(error_message)
                sys.stdout.flush()
                return [(False, error_message)]
            nomFile = brand + "_" + self.fechaArch + ".log"
            if typeLog == "MASTER":
                pathfile_log = self.path + "/" + typeLog + "/" + nomFile
                pathlib.Path(self.path + "/" + typeLog).mkdir(
                    parents=True, exist_ok=True
                )  # Creates folder before insert to log file
                result = self.write_file(pathfile_log, message_log)
                link = typeLog + "/" + nomFile

            if typeLog == "EXCHANGE_RATE":
                pathfile_log = self.path + "/" + typeLog + "/" + nomFile
                pathlib.Path(self.path + "/" + typeLog).mkdir(
                parents=True, exist_ok=True
                )  # Creates folder before insert to log file
                result = self.write_file(pathfile_log, message_log)
                link = typeLog + "/" + nomFile

            else:
                pathfile_log = self.path + "/" + typeLog + "/" + client + "/" + nomFile
                pathlib.Path(self.path + "/" + typeLog + "/" + client).mkdir(
                    parents=True, exist_ok=True
                )  # Creates folder before insert to log file
                result = self.write_file(pathfile_log, message_log)
                link = typeLog + "/" + client + "/" + nomFile
            result = nomFile
            self.S3.log_name = result
            up = self.S3.upload_object(self.log_bucket, pathfile_log, link)
            if up is not True:
                exit()
            """column records : Client|Brand|Process Name|Process Message|Status|file name"""
            records_log = [
                (client, brand, process, "prepare log", "In Progress", nomFile)
            ]
            result_db = self.db_conn.insert_log(records_log)
            # print(result_db)
            
            return result
        except Exception as e:
            raise

    def exist_file(
        self,
        typeLog: str,
        client: str,
        brand: str,
        Nomb: str  =None,
        Process: str  =None,
        tipoE: str  =None,
        Mnj: str  =None,
        module: str  =None,
        upload:bool =True,
        deep:int = 0
    ) -> bool:
        """Edits an existent log.

        Args:
            typeLog (str): type of log.
            client (str): client code.
            Nomb (str): name of log to edit.
            brand (str): processed brand.
            Process (str): process step.
            tipoE (str): Error type.
            Mnj (str): message.
            module (str): processed module.
            upload (bool): upload to s3 bucket if is available.
            deep (int): number of iteration on error.

        Returns:
            bool: True if is sucessfully edited.
        
        """
        try:
            Mnj = Mnj.replace('\"',"'",-1).replace("\n"," ",-1)
            log = f"{self.day};{self.hour};{client};{brand};{Process};{module};{tipoE};{Mnj}"
            self.S3.log_name = Nomb
            self.S3.client = client
            self.S3.exec_module = module
            self.S3.typeLog = typeLog

            if self.log_name is not None:
                Nomb = self.log_name

            if typeLog == "OPERATIONAL":
                pathfile = self.path + "/" + typeLog + "/" + client + "/" + Nomb
                pathsave = "OPERATIONAL/" + client + "/" + Nomb

            elif typeLog == "EXCHANGE_RATE":
                pathfile = self.path + "/" + typeLog + "/" + Nomb
                pathsave = "EXCHANGE_RATE/" + Nomb

            elif typeLog == "MASTER":
                pathfile = self.path + "/" + typeLog + "/" + Nomb
                pathsave = "MASTER/" + Nomb

            elif typeLog == "SYSTEM":
                pathfile = self.path + "/" + typeLog + "/" + client + "/" + Nomb
                pathsave = "SYSTEM/" + client + "/" + Nomb
            else:
                print("Wrong log type")
                exit()


            """Clean Handlers for write new line of Log"""

            if self.logger.hasHandlers():
                self.logger.handlers.clear()
            handler = logging.FileHandler(pathfile)
            self.logger.addHandler(handler)

            if tipoE == "ERROR":
                self.logger.error(log)
            elif tipoE == "INFO":
                self.logger.error(log)
            elif tipoE == "WARNING":
                self.logger.warning(log)
            elif tipoE == "CRITICAL":
                self.logger.critical(log)

            debug =  os.getenv("DEBUG")
            if debug == 'True':
                print(log)           
            else:
                print(Mnj)
            sys.stdout.flush()

            if upload:
                up = self.S3.upload_object(self.log_bucket, pathfile, pathsave)
                if up is not True:
                    exit()
            """Returns true when the process terminates successfully"""
            
            return True
        except Exception as e:
            if deep > 0:
                self.exist_file(typeLog,client,brand,Nomb,Process,"CRITICAL",f"Exception {self.print_except()} | terminating script ",module,upload=False,deep = 1)

    def print_except(self):
        """ Handles exceptions when is called
        
        Returns:
            str: exception parsed in one line for log. 
        """
        etype, value, tb = exc_info()
        info, error = format_exception(etype, value, tb)[-2:]
        info = info.replace("\n"," ",-1).replace('\"',"'",-1)
        fname = os.path.split(tb.tb_frame.f_code.co_filename)[1]
        return (f'Exception in:{info} | {error} | Line:  {tb.tb_lineno} | Name: {fname}')
    