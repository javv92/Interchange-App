import Module.GetFiles.getfiles as getfiles
import Module.Logs.logs as log
import Module.Interpretation.Mastercard.mcfiles as rmc
import Module.Interpretation.Visa.visafiles as rvisa
import Module.Ingest.Mastercard.iar_update as iar 
import Module.Ingest.Visa.ardef_update as ardef
import Module.Adapter.adapters as adapters
import exec_adapter as exec_adapter
from Module.Validation.validation import Validation
import argparse
import os
import subprocess
from datetime import datetime,timedelta
import shutil
from Module.Persistence.connection import (
    connect_to_postgreSQL as conn
)

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

if __name__ == "__main__":
    
    """This script is used to launch the main process or the interchange process.
    it can be launched manually with args.
    
    Args:
        function: function to execute [interpretation]
        -c,--client:  when function = interpretation is bank or client to search in s3
        -f,--file: when function = interpretation is  route to file in s3
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("function", help="function to execute")
    parser.add_argument(    
        "-c",
        "--client", 
        help=" when function = interpretation is bank or client to search in s3",
    )
    parser.add_argument(
        "-f", "--file", help="when function = interpretation is  route to file in s3"
    )
    args = parser.parse_args()

    def main(client: str = None, route: str = None):

        get_files = getfiles.get_files()
        client_name = "INTELICA"
        list_of_clients = get_files.get_clients(client)
        module = "OPERATIONAL"
        log_name = ''
        counter_validation_visa = 0
        counter_validation_mc = 0
        try:
            results = []
            results_visa = []
            if(len(list_of_clients) == 1):
                client_data = {}
                for clients in list_of_clients:
                    if clients["code"] == client:
                        client_data = clients
                        client_name = clients["code"]
                log_name = log.logs().new_log(
                    module, "", client, "GET FILES FROM S3 OF CLIENT", "ITX",'INTERPRETATION'
                )

                go_main = adapters.get_others(client,log_name)
                cur_date = datetime.now()
                form_date = cur_date.strftime("%Y%m%d")
                begin_date = cur_date - timedelta(3)
                begin_date = begin_date.strftime("%Y%m%d")
                form_date = cur_date + timedelta(3)
                form_date = form_date.strftime("%Y%m%d")
                go_main.config_additional_table(begin_date,form_date)
                go_main.visa_config_table_adapter_stg(begin_date)
                go_main.visa_config_table_adapter_dh(begin_date,form_date)
                go_main.mastercard_config_table_adapter_stg(begin_date)
                go_main.mastercard_config_table_adapter_dh(begin_date,form_date)

                result_list = getfiles.get_files().get_files_from_s3(
                    client_data["code"], log_name, route
                )

                for file in result_list["list_of_files"]:
                    if file["brand"] == "VI" and clients["file_visa"] == True:
                        if file["filetype"] == "IN" or file["filetype"] == "OUT": 
                            counter_validation_visa += 1
                            if file["filetype"] == "IN":
                                blocked = clients["file_block_in"]
                                ebcdic = clients["file_ebcdic_encoding_in"]
                                encoding = clients["file_encoding_in"]

                            elif file["filetype"] == "OUT":
                                blocked = clients["file_block_out"]
                                ebcdic = clients["file_ebcdic_encoding_out"]
                                encoding = clients["file_encoding_out"]


                            results_visa.append(rvisa.read_files().read_visa_file(
                            file["path"],
                            result_list["path_to_files"],
                            file["filetype"],
                            log_name,
                            clients["code"],
                            file["hash"],
                            )
                            )


                        if (
                            file["filetype"] == "VI/OTHER"
                            and clients["file_account_range"] == True
                        ):
                            ardef_files = rvisa.read_files().read_visa_ardef(
                                file["path"],
                                result_list["path_to_files"],
                                file["filetype"],
                                log_name,
                                clients["code"],
                                file["hash"],
                            )

                            if ardef_files is not None:
                                print(ardef_files)
                                res = ardef.ardef_master_update().update_from_parquet(ardef_files["parquet_info"]["local"],client,log_name)
                        

                    if file["brand"] == "MC" and clients["file_mastercard"] == True:
                        if (
                            file["filetype"] == "MC/OTHER"
                            and clients["file_iar"] == True
                        ):

                            blocked = clients["file_iar_block"]
                            ebcdic = clients["file_iar_ebcdic_encoding"]
                            encoding = clients["file_iar_encoding"]

                            iar_files = rmc.read_files().IAR_mc_read(
                                file["path"],
                                result_list["path_to_files"],
                                file["filetype"],
                                log_name,
                                clients["code"],
                                file["hash"],
                                "IP0040T1",blocked=blocked,ebcdic=ebcdic,encoding=encoding
                            )

                            if iar_files is not None:
                                res = iar.iar_master_update().update_from_parquet(iar_files["parquet_info"]["local"],client,log_name)
                                
                        if file["filetype"] == "IN" or file["filetype"] == "OUT":
                            counter_validation_mc += 1 
                            if file["filetype"] == "IN":
                                blocked = clients["file_block_in"]
                                ebcdic = clients["file_ebcdic_encoding_in"]
                                encoding = clients["file_encoding_in"]

                            elif file["filetype"] == "OUT":
                                blocked = clients["file_block_out"]
                                ebcdic = clients["file_ebcdic_encoding_out"]
                                encoding = clients["file_encoding_out"]

                            results.append(rmc.read_files().read_mc_file(
                                    file["path"],
                                    result_list["path_to_files"],
                                    file["filetype"],
                                    log_name,
                                    clients["code"],
                                    file["hash"],
                                    blocked=blocked,
                                    ebcdic=ebcdic,
                                    encoding=encoding,
                                )
                            )

                log.logs().exist_file(
                    "OPERATIONAL",
                    clients["code"],
                    "VISA AND MASTERCARD",
                    log_name,
                    "END OF INTERPRETATION",
                    "INFO",
                    "Closing interpretation",
                    "INTERPRETATION"
                )

                arg1=''
                arg1_count = 0
                list_of_files = []
                for value in results:
                    if value["status"] != "REVISION" and value["on_error"] == False:
                        dict_of_file = {}
                        dict_of_file['header_date'] = str(value["parquet_info"]['file_header'])
                        dict_of_file['brand'] = 'mastercard'
                        dict_of_file['type_file'] = str(value["parquet_info"]['type']).lower()
                        dict_of_file['parquet_file'] = value["parquet_info"]['local']
                        dict_of_file['hash_file'] = value["parquet_info"]['hash']
                        dict_of_file['number_file'] = str(arg1_count)
                        list_of_files.append(dict_of_file)
                        arg1_count+=1

                for value in results_visa:
                    if value["status"] != "REVISION" and value["on_error"] == False:
                        dict_of_file = {}
                        dict_of_file['header_date'] = str(value["parquet_info"]['file_header'])
                        dict_of_file['brand'] = 'visa'
                        dict_of_file['type_file'] = str(value["parquet_info"]['type']).lower()
                        dict_of_file['parquet_file'] = value["parquet_info"]['local']
                        dict_of_file['hash_file'] = value["parquet_info"]['hash']
                        dict_of_file['number_file'] = str(arg1_count)
                        list_of_files.append(dict_of_file)
                        arg1_count+=1
                
                if arg1_count > 0:
                    adapter = exec_adapter.exec_adapter(client_name,log_name)
                    process = adapter.parallel_execution(list_of_files)

                log.logs().exist_file(
                    "OPERATIONAL",
                    clients["code"],
                    "VISA AND MASTERCARD",
                    log_name,
                    "END OF GENERAL PROCESS",
                    "INFO",
                    "Closing process",
                    "INTERPRETATION"
                )

                shutil.rmtree(f"FILES/{client_name}")
                
            else:
                print("Client doesnt exists")
        except Exception as e:
            if log_name != '':
                log.logs().exist_file(
                "OPERATIONAL",
                client_name,
                "VISA AND MASTERCARD",
                log_name,
                "END OF INTERPRETATION",
                "CRITICAL",
                f"UNHANDLED EXCEPTION: {str(log.logs().print_except())}",
                "INTERPRETATION"
            )
            else:
                print(str(log.logs().print_except()))

        else:
            end_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            for file in result_list["list_of_files"]:
                updating = conn().update(
                    "CONTROL.T_CONTROL_FILE",
                    f"WHERE process_file_name = '{file['path']}'",
                    {
                        "app_end_date": end_timestamp
                    }
                )

    if(args.function == "interpretation"):
        if args.client and args.file:
            main(args.client, args.file)
        else:
            if not args.client:
                print("Client is missing")
            if not args.file:
                print("File route is missing")