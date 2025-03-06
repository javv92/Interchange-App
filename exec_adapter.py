from pathlib import Path
from Module.Adapter.adapters import get_adapters, get_others
from Module.Validation.validation import Validation
from datetime import datetime
import sys, os
import argparse
import concurrent.futures
import Module.Logs.logs as log
import pandas as pd
import Module.Logs.logs as log

path = Path(__file__).resolve().parent.parent.parent
sys.path.append(os.path.join(os.getcwd(), str(path)))

class exec_adapter:
    """Class to execute the adapter Module.
    Params:
        customer_code (str): customer code.
        log_file: log file name.
    """

    def __init__(self,customer_code:str,log_file:str) -> None:

        self.customer_code = customer_code
        self.log_file = log_file
        pass

    

    def execution_config_adapter(self,customer_code:str,string_date:str,string_end_date:str = None):
        """Method for execution of adapter sequence.
        
        Args:
            customer_code (str): client code.
            string_date (str):  start date
            string_end_date (str): end date 

        """
        if string_end_date == None:
            string_end_date = string_date
        ga_main = get_others(customer_code,'ITX_test.log')
        print(ga_main.visa_config_table_adapter_stg(string_date))
        print(ga_main.visa_config_table_adapter_dh(string_date,string_end_date))
        print(ga_main.mastercard_config_table_adapter_stg(string_date))
        print(ga_main.mastercard_config_table_adapter_dh(string_date,string_end_date))
        print(ga_main.config_additional_table(string_date,string_end_date))

    def execution_adapter(self,dict_process:dict):
        """Execution of adapter
        
        Args:
            dict_process (dict): dictionary with files info for adapters.
        """
        try:
            if dict_process['brand'] == 'visa':
                ga = get_adapters(self.customer_code,self.log_file)
                go = get_others(self.customer_code,self.log_file)
                ga.visa_upload_adapter(dict_process['parquet_file'],dict_process['type_file'],dict_process['hash_file'],dict_process['number_file'])
                log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "VISA",
                        self.log_file,
                        "END OF ADAPTER",
                        "INFO",
                        "Closing adapter file : " + dict_process['hash_file'],
                        "ADAPTER"
                    )
            if dict_process['brand'] == 'mastercard':
                ga = get_adapters(self.customer_code,self.log_file)
                go = get_others(self.customer_code,self.log_file)
                ga.mastercard_upload_adapter(dict_process['parquet_file'],dict_process['type_file'],dict_process['hash_file'],dict_process['number_file'])
                log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "MASTERCARD",
                        self.log_file,
                        "END OF ADAPTER",
                        "INFO",
                        "Closing adapter file : " + dict_process['hash_file'],
                        "ADAPTER"
                    )

        except Exception as e:
            log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_file,
                "END OF ADAPTER",
                "CRITICAL",
                f"UNHANDLED EXCEPTION: {str(log.logs().print_except())}",
                "ADAPTER"
            )

    def execution_interchange_rules(self,dict_process:dict):
        """ execution of intechange rules

        Args:
            dict_process (dict): dictionary with files info for adapters.
        """
        try:
            if dict_process['brand'] == 'visa':
                ga = get_adapters(self.customer_code,self.log_file)
                go = get_others(self.customer_code,self.log_file)
                go.load_visa_interchange(dict_process['header_date'],dict_process['type_file'],dict_process['hash_file'],dict_process['number_file'])
                go.load_visa_sms_interchange(dict_process['header_date'],dict_process['type_file'],dict_process['hash_file'],dict_process['number_file'])
                log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "VISA",
                        self.log_file,
                        "END OF ADAPTER",
                        "INFO",
                        "Closing interchange file : " + dict_process['hash_file'],
                        "INTERCHANGE"
                    )
            if dict_process['brand'] == 'mastercard':
                ga = get_adapters(self.customer_code,self.log_file)
                go = get_others(self.customer_code,self.log_file)
                go.load_mastercard_interchange(dict_process['header_date'],dict_process['type_file'],dict_process['hash_file'],dict_process['number_file'])
                log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "MASTERCARD",
                        self.log_file,
                        "END OF ADAPTER",
                        "INFO",
                        "Closing interchange file : " + dict_process['hash_file'],
                        "INTERCHANGE"
                    )
        
        except Exception as e:
            log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_file,
                "END OF INTERCHANGE",
                "CRITICAL",
                f"UNHANDLED EXCEPTION: {str(log.logs().print_except())}",
                "INTERCHANGE"
            )

    def execution_load(self,dict_process:dict):
        # ToDo: Evaluar eliminar este metodo no se utiliza y solo se ejecuta por tipo de archivo
        """ execution of load of transactions

        Args:
            dict_process (dict): dictionary with files info for adapters.
        """
        try:
            if dict_process['brand'] == 'visa':
                ga = get_adapters(self.customer_code,self.log_file)
                go = get_others(self.customer_code,self.log_file)
                go.visa_load_transaction(type_file=dict_process['type_file'], hash_files=dict_process['hash_files'])
                log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "VISA",
                        self.log_file,
                        "END OF INGEST",
                        "INFO",
                        "Closing ingest type file : " + dict_process['type_file'],
                        "INGEST"
                    )
            if dict_process['brand'] == 'mastercard':
                ga = get_adapters(self.customer_code,self.log_file)
                go = get_others(self.customer_code,self.log_file)
                go.mastercard_load_transaction(type_file=dict_process['type_file'],
                                               hash_files=dict_process['hash_files'])
                log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "MASTERCARD",
                        self.log_file,
                        "END OF INGEST",
                        "INFO",
                        "Closing ingest type file : " + dict_process['type_file'],
                        "INGEST"
                    )
        except Exception as e:
            log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_file,
                "END OF INGEST",
                "CRITICAL",
                f"UNHANDLED EXCEPTION: {str(log.logs().print_except())}",
                "INGEST"
            )

    def execution(self,dict_process:dict):
        """ execution adapters sequence
        Args:
            dict_process (dict): dictionary with files info for adapters.
        """
        try:
            if dict_process['brand'] == 'visa':
                ga = get_adapters(self.customer_code,self.log_file)
                go = get_others(self.customer_code,self.log_file)
                go.visa_load_transaction(dict_process['header_date'],dict_process['type_file'],
                                         hash_files=dict_process['hash_files'])
                go.visa_load_calculated_field_dh(dict_process['header_date'],dict_process['type_file'],
                                                 hash_files=dict_process['hash_files'])
                go.visa_load_sms_calculated_field_dh(dict_process['header_date'],dict_process['type_file'],
                                                     hash_files=dict_process['hash_files'])
                go.visa_load_vss_110_calculated_field_dh(dict_process['header_date'],dict_process['type_file'],
                                                         hash_files=dict_process['hash_files'])
                go.visa_load_vss_120_calculated_field_dh(dict_process['header_date'],dict_process['type_file'],
                                                         hash_files=dict_process['hash_files'])
                go.visa_load_vss_130_calculated_field_dh(dict_process['header_date'],dict_process['type_file'],
                                                         hash_files=dict_process['hash_files'])
                go.visa_load_vss_140_calculated_field_dh(dict_process['header_date'],dict_process['type_file'],
                                                         hash_files=dict_process['hash_files'])
                log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "VISA",
                        self.log_file,
                        "END OF INGEST",
                        "INFO",
                        "Closing ingest type file : " + dict_process['type_file'],
                        "INGEST"
                    )
            if dict_process['brand'] == 'mastercard':
                ga = get_adapters(self.customer_code,self.log_file)
                go = get_others(self.customer_code,self.log_file)
                go.mastercard_load_transaction(dict_process['header_date'], type_file=dict_process['type_file'],
                                               hash_files=dict_process['hash_files'])
                go.mastercard_load_calculated_field_dh(dict_process['header_date'], type_file=dict_process['type_file'],
                                                       hash_files=dict_process['hash_files'])
                go.mastercard_load_exclusion_transaction(dict_process['header_date'],type_file=dict_process['type_file'])
                log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "MASTERCARD",
                        self.log_file,
                        "END OF INGEST",
                        "INFO",
                        "Closing ingest type file : " + dict_process['type_file'],
                        "INGEST"
                    )
        except Exception as e:
            log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_file,
                "END OF INGEST",
                "CRITICAL",
                f"UNHANDLED EXCEPTION: {str(log.logs().print_except())}",
                "INGEST"
            )
    
    def execution_validation(self,dict_process:dict):
        """ execution of validation
        Args:
            dict_process (dict): dictionary with files info for adapters.
        """
        try:
            if dict_process['brand'] == 'visa':
                v = Validation(dict_process['brand'],self.customer_code,self.log_file)
                v.process_validation_visa_interchange(dict_process['header_date'],dict_process['type_file'])
                if dict_process['type_file'] == 'in':
                    v.process_validation_visa_sms_interchange(dict_process['header_date'],dict_process['type_file'])
                log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "VISA",
                        self.log_file,
                        "END OF VALIDATION",
                        "INFO",
                        "Closing validation type file : " + dict_process['type_file'],
                        "VALIDATION"
                    )
            if dict_process['brand'] == 'mastercard':
                v = Validation(dict_process['brand'],self.customer_code,self.log_file)
                v.process_validation_mastercard_interchange(dict_process['header_date'],dict_process['type_file'])
                log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "MASTERCARD",
                        self.log_file,
                        "END OF VALIDATION",
                        "INFO",
                        "Closing validation type file : " + dict_process['type_file'],
                        "ADAPTER"
                    )
        except Exception as e:
            log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_file,
                "END OF VALIDATION",
                "CRITICAL",
                f"UNHANDLED EXCEPTION: {str(log.logs().print_except())}",
                "VALIDATION"
            )

    def parallel_execution(self,list_args:list):
        """ execution in parallel of methods.

        Args:
            list_args (list): list of files.
        """
        df_dict = pd.DataFrame(list_args)
        g_adapter = get_adapters(self.customer_code,self.log_file)
        df_dict_distinct = df_dict[['brand','header_date','type_file']].drop_duplicates()
        list_args_agg = []
        g_adapter.visa_clear_stg()
        g_adapter.mastercard_clear_stg()
        for value in df_dict_distinct.values:
            hash_files = df_dict.loc[(df_dict['brand'] == value[0]) & (df_dict['header_date'] == value[1]) & (
                        df_dict['type_file'] == value[2])]['hash_file']

            list_args_agg.append({
                'brand': value[0],
                'header_date': value[1],
                'type_file': value[2],
                'hash_files': list(hash_files.values)
            })

        log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA AND MASTERCARD",
            self.log_file,
            "EXECUTION PARALLEL",
            "INFO",
            "list file execute : " + '|'.join([','.join([value['brand'], value['header_date'], value['type_file'],
                                                         value['hash_file']]) for value in list_args]),
            "ADAPTER"
        )

        with concurrent.futures.ProcessPoolExecutor(8) as executor:
            executor.map(self.execution_adapter,list_args)
        
        with concurrent.futures.ProcessPoolExecutor(8) as executor:
            executor.map(self.execution,list_args_agg)
        
        with concurrent.futures.ProcessPoolExecutor(8) as executor:
            executor.map(self.execution_interchange_rules,list_args)


if __name__ == '__main__':
    """This part of the script is used to launch manually the adapters module.
    it can be launched with args.
    
    Args:
        function: function to execute [adapter,config_adapter]
        -c,--customer_code: when function = adapter must be the name of the project or client
        -l,--log_file: when function = adapter must be visa or mastercard
        -d,--dict_file: when function = adapter must be a dict list of the config process file
        -start_dt,--start_date: when function = start date parameter
        -end_dt,--end_date: when function = end date parameter
    """
            
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("function", help="function to execute")
    parser.add_argument("-c", "--customer_code", help="when function = adapter must be the name of the project or client, example: BRDRO")
    parser.add_argument(
        "-l",
        "--log_file",
        help=" when function = adapter must be visa or mastercard",
    )
    parser.add_argument("-d", "--dict_file", help="when function = adapter must be a dict list of the config process file")
    parser.add_argument("-start_dt", "--start_date", help="when function = start date parameter, example: 20221011")
    parser.add_argument("-end_dt", "--end_date", help="when function = end date parameter, example: 20221031")

    args = parser.parse_args()
    log_temp_name = log.logs().new_log(
                "OPERATIONAL", "", args.customer_code, "GET FILES FROM S3 OF CLIENT", "VISA AND MASTERCARD",'ADAPTER'
            )
    if(args.function == "adapter"):
        if args.customer_code and args.log_file and args.dict_file:    
                list_dict = []
                ga_main = get_adapters(args.customer_code,args.log_file)
                go_main = get_others(args.customer_code,args.log_file)
                exec_adap = exec_adapter(args.customer_code,args.log_file)
                list_dict.extend(eval(args.dict_file))
                ga_main.visa_clear_stg()
                ga_main.mastercard_clear_stg()
                exec_adap().parallel_execution(list_dict)
                exit()
    if(args.function == 'config_adapter'):
        if args.customer_code and args.start_date and args.customer_code.isupper():
                str_end_date = args.end_date
                str_start_date = args.start_date
                if args.end_date == None:
                    str_end_date = args.start_date
                go_main = get_others(args.customer_code,log_temp_name)
                go_main.config_additional_table(str_start_date,str_end_date)
                go_main.visa_config_table_adapter_stg(str_start_date)
                go_main.visa_config_table_adapter_dh(str_start_date,str_end_date)
                go_main.mastercard_config_table_adapter_stg(str_start_date)
                go_main.mastercard_config_table_adapter_dh(str_start_date,str_end_date)
        else:
            print('Check that customer code is entirely uppercase')