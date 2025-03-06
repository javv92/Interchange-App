import os
import csv
import time
import ebcdic
import io
import struct
import binascii
import pathlib
import glob
import pandas as pd
import Module.Interpretation.Mastercard.dataelements as de
import Module.Logs.logs as log
from Module.Persistence.connection import (
    connect_to_postgreSQL as conn,
    connect_to_s3 as s3,
)
from typing import List
from datetime import datetime


class read_files:
    """Class for read and interpretation of mastercard interchange files."""
    def __init__(self):
        pass

    def read_mc_file(
        self,
        path_to_file: str,
        parent_directory: str,
        type: str,
        log_name: str,
        client: str,
        hash: str,
        blocked: bool = True,
        ebcdic: bool = False,
        encoding: str = None,
    ) -> dict:
        """Method to start decoding MC transaction files

        Args:
            path_to_file (str): path to local file.
            parent_directory (str): path to execution folder.
            type (str): type of file.
            log_name (str): name of log file.
            client (str): client name.
            hash (str): hash code of file.
            blocked (bool): True if file is blocked in 1014 bytes string.
            ebcdic (bool): True if file is encoded in ebcdic, False if is binary.
            encoding (str): if 'None' and ebcdic = True then cp500 else if 'None' and ebcdic = False then 'Latin-1'.

        Returns:
            dict : dictionary of file data.

        """
        try:
            if ebcdic == True and encoding == None:
                encoding = "cp500"
            elif ebcdic == False and encoding == None:
                encoding = "Latin-1"
            step = "INTERPRETATION OF FILE"
            module = "INTERPRETATION"
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                "opening file: "
                + path_to_file
                + " |Blocked: "
                + str(blocked)
                + " |ebcdic: "
                + str(ebcdic)
                + " |encoding: "
                + encoding,
                module
            )
            file = open(path_to_file, "br")
            filezise = os.path.getsize(path_to_file)
            bufferfile = io.BytesIO(file.read(-1))

            if blocked:
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    "unblocking file",
                    module
                )
                unblocked_str = self.unblock_file(bufferfile, filezise)
                unblocked_str = io.BytesIO(unblocked_str)
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    "file is now unblocked",
                    module
                )
            else:
                unblocked_str = bufferfile
            file.close()
            self.path_to_file_destiny = parent_directory + "/" + "RESULT/MASTERCARD/" + type
            pathlib.Path(self.path_to_file_destiny).mkdir(parents=True, exist_ok=True)
            parameters = de.Parameters().getdataelements()
            reads = True
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                "Reading file: " + path_to_file,
                module
            )
            messages = 0
            on_error = False
            finished_out = None
            file_finger_print = time.time_ns()
            destiny_file = "000101"
            status = "IN PROGRESS"

            row_file = []

            while reads:
                try:
                    raw_record = unblocked_str.read(4)
                    try:
                        record_length = struct.unpack(">i", raw_record)[0]
                    except Exception as e:
                        record_length = 0

                    if record_length == 0:
                        reads = False
                        break

                    message_total = unblocked_str.read(20)
                    if len(message_total) != 20:
                        continue
                    message_type_indicator, binary_bitmap = struct.unpack(
                        "4s16s", message_total
                    )

                    header = False
                    if message_type_indicator.decode(encoding) == "1644":
                        header = True
                        header_of_message = False

                    bitmap = self.get_bitmaps(binary_bitmap)

                    if unblocked_str.tell() == (unblocked_str.getbuffer().nbytes):
                        reads = False
                        break
                    write_to_file_list = [message_type_indicator.decode(encoding)]
                    for i in self.nums(2, 128):
                        de_value = b""
                        if i in bitmap:
                            if i > 1:
                                de_length = 0
                                if parameters[i]["fixed"]:
                                    de_length = parameters[i]["length"]
                                    de_value = unblocked_str.read(de_length)
                                else:
                                    _re_num = unblocked_str.read(
                                        parameters[i]["length"]
                                    ).decode(encoding)
                                    de_length = int((_re_num))
                                    de_value = unblocked_str.read(de_length)
                                    if i == 55:
                                        de_value = self.icc_to_somethingreadable(de_value)
                                if (
                                    header
                                    and i == 24
                                    and de_value.decode(encoding) == "697"
                                ):
                                    finished_out = None
                                    header_of_message = True
                                if header and i == 48 and header_of_message:
                                    destiny_file = self.look_for_date(
                                        de_value.decode(encoding)
                                    )
                                    header_of_message = False
                                    header = False

                        else:
                            de_value = b""
                        write_to_file_list.append(de_value.decode(encoding))
                    write_to_file_list.extend([client, hash, destiny_file])

                    if messages == 0:
                        date_of_file = datetime.strptime(destiny_file, "%y%m%d")
                        date_for_name = datetime.strptime(destiny_file, "%y%m%d").strftime("%Y%m%d")
                        date_formated_as = date_of_file.strftime("%Y-%m-%d")
                        # checking if is reprocesing
                        check_reprocces = conn().select(
                            "CONTROL.T_CONTROL_FILE",
                            f"""WHERE file_type = '{type}' and customer = '{client}' and brand = 'MC'
                            and code = '{hash}'
                            """,
                            "count(code) as file",
                        )

                        if check_reprocces[0]["file"] > 1:
                            status = "REVISION"
                            log.logs().exist_file(
                                "OPERATIONAL",
                                client,
                                "MASTERCARD",
                                log_name,
                                step,
                                "WARNING",
                                "This file has already been checked",
                                module
                            )

                        updating = conn().update(
                            "CONTROL.T_CONTROL_FILE",
                            f"WHERE process_file_name = '{path_to_file}'",
                            {
                                "description_status": status,
                                "records_number": str(messages),
                                "file_date": date_of_file,
                                "control_message": "Interpretation",
                            },
                        )
                        if updating[0]:
                            message_type = "INFO"
                            message = updating[1]["Message"]
                        else:
                            message_type = "CRITICAL"
                            message = (
                                "status cannot be updated, Exception: "
                                + updating[1]["Message"]
                            )

                        header = (
                            "MESSAGE_TYPE|"
                            + "|".join("DE" + str(i) for i in self.nums(2, 128))
                            + "|app_client|app_hash|app_file_date"
                        )
                        header = header.split('|')
                        if status == "REVISION":
                            self.path_to_file_destiny = (
                                self.path_to_file_destiny + "/REVISION"
                            )
                        pathlib.Path(self.path_to_file_destiny + "/" + destiny_file).mkdir(
                            parents=True, exist_ok=True
                        )
                        row_file.append(header)
                    row_file.append(write_to_file_list)
                    messages += 1
                except KeyError as ke:
                    reads = False
                    on_error = True
                    if finished_out != None:
                        finished_out.close()
                        os.remove(
                            self.path_to_file_destiny
                            + "/"
                            + destiny_file
                            + "/"
                            + destiny_file
                            + str(file_finger_print)
                            + ".csv"
                        )
                    log.logs().exist_file(
                        "OPERATIONAL",
                        client,
                        "MASTERCARD",
                        log_name,
                        step,
                        "WARNING",
                        "Error reading file: "
                        + path_to_file
                        + " | readed messages: "
                        + str(messages)
                        + " | KeyException on parameters: '"
                        + str(ke)
                        + "' key is not founded in DataElements, Bad binary map",
                        module
                    )

                except ValueError as e:
                    reads = False
                    on_error = True
                    if finished_out != None:
                        finished_out.close()
                        os.remove(
                            self.path_to_file_destiny
                            + "/"
                            + destiny_file
                            + "/"
                            + destiny_file
                            + str(file_finger_print)
                            + ".csv"
                        )
                    log.logs().exist_file(
                        "OPERATIONAL",
                        client,
                        "MASTERCARD",
                        log_name,
                        step,
                        "WARNING",
                        "Error reading file: "
                        + path_to_file
                        + " | readed messages: "
                        + str(messages)
                        + " | exception reading file: "
                        + str(e),
                        module
                    )
                except Exception as e:
                    reads = False
                    on_error = True
                    if finished_out != None:
                        finished_out.close()
                    os.remove(
                        self.path_to_file_destiny
                        + "/"
                        + destiny_file
                        + "/"
                        + destiny_file
                        + str(file_finger_print)
                        + ".csv"
                    )
                    log.logs().exist_file(
                        "OPERATIONAL",
                        client,
                        "MASTERCARD",
                        log_name,
                        step,
                        "WARNING",
                        "Error reading file: "
                        + path_to_file
                        + " | readed messages: "
                        + str(messages)
                        + " | exception reading file: "
                        + str(log.logs().print_except()),
                        module
                    )

            if on_error:
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "MASTERCARD",
                    log_name,
                    step,
                    "ERROR",
                    "Closed reading of file with error: " + path_to_file,
                    module
                )
                status = "ERROR"
                messages = 0
            else:
                csv_path = f'{self.path_to_file_destiny}/{destiny_file}/{destiny_file}{str(file_finger_print)}.csv'
                with open(csv_path, "a", encoding="Latin-1", newline='') as finished_out:
                    csv_writer = csv.writer(finished_out, delimiter='|', quoting=csv.QUOTE_ALL, escapechar='\\')
                    csv_writer.writerows(row_file)
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    "Finished read of file: "
                    + path_to_file
                    + " | readed messages: "
                    + str(messages),
                    module
                )
                if status == "IN PROGRESS":
                    status = "PROCESSED"

            updating = conn().update(
                "CONTROL.T_CONTROL_FILE",
                f"WHERE process_file_name = '{path_to_file}'",
                {
                    "description_status": status,
                    "records_number": str(messages),
                    "control_message": "Closed",
                },
            )
            if updating[0]:
                message_type = "INFO"
                message = updating[1]["Message"]
            else:
                message_type = "CRITICAL"
                message = "status cannot be updated, Exception: " + updating[1]["Message"]
            
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                "UPDATING STATUS OF FILE",
                message_type,
                "UPDATE STATUS: " + message,
                module
            )
            csv_name = self.path_to_file_destiny + "/" + destiny_file + "/"+ destiny_file + str(file_finger_print)+ ".csv"
            if (on_error):
                parquet_file = []
            else:
                parquet_file = Utils().generate_parquet_for_mc(parent_directory,date_for_name,csv_name,type,hash,client,status,log_name)
            
            return {"result": True, "message": "Finished", "on_error": on_error, "parquet_info":parquet_file, "status":status }
        except Exception as e:
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA AND MASTERCARD",
                log_name,
                "END OF INTERPRETATION",
                "CRITICAL",
                f"UNHANDLED EXCEPTION: {str(log.logs().print_except())}",
                "INTERPRETATION"
            )

    def __convert_to_binary(self, char: str) -> str:
        """Returns binary rep of char
        
        Args:
            char (str): char to be converted

        Returns:
            res (str): binary representation. 
        
        """
        fmt = ("{:02x} " * len(char))[:-1]
        ini_string = fmt.format(*char)
        res = f"{int(ini_string,16):08b}"
        return res

    def unblock_file(self, fileobj: str, sizeinbytes: int) -> str:
        """Unblocks files blocked in 1014 bytes, returns binary string without 2 last bytes every 1014 bytes

        Args:
            fileobj (str): readed file.
            sizeinbytes (int): file size.

        Returns:
            result (str): unblocked file string.
           
        """

        fileobj.seek(0)
        chunk_array = bytearray()

        while True:

            chunk = fileobj.read(1012)
            chunk_array.extend(chunk)

            block_separator = fileobj.read(2)
            if block_separator not in [bytes([]), bytes([0x20, 0x20])]:
                fileobj.seek(fileobj.tell() - 2)

            if len(chunk) < 1012:
                break

        result = bytes(chunk_array)
        return result

    def get_bitmaps(self, bitmap: str) -> List[int]:
        """gets dataelemnts present in bitmap
        
        Args:
            bitmap (str): bitmap as string.

        Returns:
            array_elements (list):ist of dataelements.
        """
        array_elements = []
        DE = 1
        for i in range(0, len(bitmap)):
            b = bitmap[i : i + 1]
            res = self.__convert_to_binary(b)
            for j in range(0, len(res)):
                if res[j : j + 1] == "1":
                    array_elements.append(DE)
                DE = DE + 1

        return array_elements

    def nums(self, first_number: int, last_number: int, step:int =1) -> range:
        """returns range of number between first and last number
        
        Args:
            first_number (int): start number.  
            last_number (int): last number of range.
            step (int): steps for loop.
        
        Returns:
            range: range from first_number to last_number by step
        """
        return range(first_number, last_number + 1, step)

    def look_for_date(self, de: str) -> str:
        """Looks for date in file header
        
        Args:
            de: dataelement string
        
        Returns:
            get_date (str): date in format yymmdd or 010101
        """
        readed = 0
        while readed < len(de):
            get_pds = de[readed : readed + 4]
            readed = readed + 4
            get_size = de[readed : readed + 3]
            readed = readed + 3
            if get_pds == "0105":
                readed = readed + 3
                get_date = de[readed : readed + 6]
                return get_date
            else:
                readed = readed + int(get_size)
        return "010101"

    def icc_to_somethingreadable(self, message: str) -> str:
        """Function to read DE55
        
        Args:
            message: message to decode
        
        Returns:
            values (str): decoded message in hex
        """
        # Exists 2 bytes
        bytes_prefixes = [b"\x9f", b"\x5f"]
        values = binascii.b2a_hex(message)

        return values

    def IAR_mc_read(
        self,
        path_to_file: str,
        parent_directory: str,
        type: str,
        log_name: str,
        client: str,
        hash: str,
        table_to_look: str,
        blocked: bool = True,
        ebcdic: bool = False,
        encoding: str = None,
    ):
        """IAR read method, works similar to the interchange file interpreter.
        This method works for table "IP0040T1", but can be configured to use more tables.
        Must have a config dict to work properly.
        Also, this method depends on the RWD or message lenght from the first 4 characters of each message

        Args:
            path_to_file (str): path to local file.
            parent_directory (str): path to execution folder.
            type (str): type of file.
            log_name (str): name of log file.
            client (str): client name.
            hash (str): hash code of file.
            blocked (bool): True if file is blocked in 1014 bytes string.
            ebcdic (bool): True if file is encoded in ebcdic, False if is binary.
            encoding (str): if 'None' and ebcdic = True then cp500 else if 'None' and ebcdic = False then 'Latin-1'.

        Returns:
            dict: dictionary with file data.
        """
        module = "INTERPRETATION"
        step = "INTERPRETATION OF IAR FILE"
        try:
            file = open(path_to_file, "br")
            filezise = os.path.getsize(path_to_file)
            bufferfile = io.BytesIO(file.read(-1))
            counter = 0
            if ebcdic == True and encoding == None:
                encoding = "cp500"
            elif ebcdic == False and encoding == None:
                encoding = "Latin-1"

            if blocked:
                unblocked_str = self.unblock_file(bufferfile, filezise)
                unblocked_str = io.BytesIO(unblocked_str)
            else:
                unblocked_str = bufferfile
            file.close()
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                "Start read of file: "
                + path_to_file
                + " |Blocked: "
                + str(blocked)
                + " |ebcdic: "
                + str(ebcdic)
                + " |encoding: "
                + encoding,
                module
            )
            # Get header date
            # Tries to get the header date, when is an update file is tested, but no when is a replacement file
            params = de.Parameters().getIPMParameters()

            reader_next_jump = struct.unpack(">i", unblocked_str.read(4))[0]
            reader_position = unblocked_str.read(reader_next_jump).decode(encoding)
            if len(reader_position) == 27:
                header_config = params["update_header"]
                title = reader_position[
                    header_config["header"]["header_title"]["start"] : header_config[
                        "header"
                    ]["header_title"]["end"]
                ]
                date = reader_position[
                    header_config["header"]["header_date"]["start"] : header_config[
                        "header"
                    ]["header_date"]["end"]
                ]
                htime = reader_position[
                    header_config["header"]["header_time"]["start"] : header_config[
                        "header"
                    ]["header_time"]["end"]
                ]
                
                dobj = datetime.strptime(str(date), "%Y%m%d")


            elif len(reader_position) == 80:
                header_config = params["replace_header"]
                title = reader_position[
                    header_config["header"]["header_title"]["start"] : header_config[
                        "header"
                    ]["header_title"]["end"]
                ]
                date = reader_position[
                    header_config["header"]["header_date"]["start"] : header_config[
                        "header"
                    ]["header_date"]["end"]
                ].replace("/", "")
                htime = reader_position[
                    header_config["header"]["header_time"]["start"] : header_config[
                        "header"
                    ]["header_time"]["end"]
                ].replace(":", "")
                dobj = datetime.strptime(date.strip(), "%m%d%y")
            else:
                dobj = datetime.now()
                title = "unknow_type"
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "MASTERCARD",
                    log_name,
                    step,
                    "ERROR",
                    "This file have an unknow header, please check the file",
                    module
                )
                return False

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                f"File header indicates {path_to_file} is a {title} type file",
                module
            )
            # Get Keys of file
            # This part gets the tables sub keys (wich are distinct from the tables names key) represented by a 3 chars string
            # All of this is similar to a list of tables presents in the file and all must be under the the same table table layout, in this case
            # "IP0000T1"
            # This part iterates until a trailer record is found
            # The table is readed in its compresed form!!!!
            Keys = True
            key_config = params["key"]
            tables = dict()
            records_exists = False
            while Keys:
                reader_next_jump = struct.unpack(">i", unblocked_str.read(4))[0]
                reader_position = unblocked_str.read(reader_next_jump).decode(encoding)
                key = reader_position[
                    key_config["key"]["start"] : key_config["key"]["end"]
                ]
                if key != key_config["layout"]:
                    if reader_position.startswith("TRAILER RECORD IP0000T1"):
                        records_exists = True
                    Keys = False
                    break
                table = reader_position[
                    key_config["table_ipm_id"]["start"] : key_config["table_ipm_id"][
                        "end"
                    ]
                ]
                sub_id = reader_position[
                    key_config["table_sub_id"]["start"] : key_config["table_sub_id"][
                        "end"
                    ]
                ]

                tables[sub_id] = table

            if records_exists == False:
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "MASTERCARD",
                    log_name,
                    step,
                    "ERROR",
                    f"No IP0000T1 trailer records founded in file {path_to_file}",
                    module
                )
                return False

            if table_to_look not in tables.values():
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "MASTERCARD",
                    log_name,
                    step,
                    "ERROR",
                    f"No {table_to_look} is present in file {path_to_file}",
                    module
                )
                return False

            # reading records
            # If the looking table is present in the list of keys, now is time to start to looking for the values
            # for convenience the trailer header line is skiped and only values are captured.
            # Also the identifier for the record value is the sub key, not the table key (Tested and compared)
            records = True
            record_config = params["record"]
            messages = []

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Reading record in file {path_to_file}",
                module
            )
            destiny_file = dobj.strftime("%y%m%d")
            date_for_name = dobj.strftime("%Y%m%d")
            while records:
                try:
                    reader_next_jump = struct.unpack(">i", unblocked_str.read(4))[0]
                except Exception as e:
                    reader_next_jump = 0

                if reader_next_jump == 0:
                    records = False
                    break
                reader_position = unblocked_str.read(reader_next_jump).decode(encoding)
                record_table_id = reader_position[
                    record_config["start"] : record_config["end"]
                ]
                if tables.get(record_table_id) == table_to_look:
                    table_params = params["tables"][table_to_look]
                    line = dict()
                    for key in table_params:
                        start = table_params[key]["start"]
                        end = table_params[key]["end"]
                        line[key] = reader_position[start:end]
                    line["app_full_data"] = reader_position
                    line["app_processing_date"] = date_for_name
                    line["app_header_type"] = title.strip()
                    line["app_customer_code"] = client
                    line["app_hash_file"] = hash
                    line["app_type_file"] = "IAR"
                    messages.append(line)

            d = pd.DataFrame(messages)
            counter = len(d.index)
            destiny_directory = (
                parent_directory + "/RESULT/MASTERCARD/IAR/" + destiny_file
            )
            pathlib.Path(destiny_directory).mkdir(parents=True, exist_ok=True)
            d.to_csv(
                destiny_directory
                + "/"
                + destiny_file
                + "_"
                + title.strip().replace(" ", "_")
                + ".csv",
                index=False,
                sep="|",
            )
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Finished reading of file {path_to_file}",
                module
            )
            status = "PROCESSED"
            updating = conn().update(
                "CONTROL.T_CONTROL_FILE",
                f"WHERE process_file_name = '{path_to_file}'",
                {
                    "description_status": status,
                    "records_number": len(d),
                    "control_message": "Closed",
                },
            )
            if updating[0]:
                message_type = "INFO"
                message = updating[1]["Message"]
            else:
                message_type = "CRITICAL"
                message = (
                    "status cannot be updated, Exception: " + updating[1]["Message"]
                )
            step ="UPDATING STATUS OF FILE"
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                message_type,
                "Updated status: " + message,
                module
            )
            csv_name = destiny_directory+ "/"+ destiny_file+ "_"+ title.strip().replace(" ", "_")+ ".csv"
            parquet_file = Utils().generate_parquet_for_mc(parent_directory,date_for_name,csv_name,"IAR",hash,client,status,log_name)
            
            return {"result": True, "message": "Finished", "parquet_info":parquet_file, "status":status}

        except Exception as e:

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                "INTERPRETATION OF IAR FILE",
                "WARNING",
                "Error reading file: "
                + path_to_file
                + " | readed messages: "
                + str(counter)
                + " | exception reading file: "
                + str(e),
                module
            )

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                "INTERPRETATION OF IAR FILE",
                "ERROR",
                "Closed reading of file with error: " + path_to_file,
                module
            )
            status = "ERROR"

            updating = conn().update(
                "CONTROL.T_CONTROL_FILE",
                f"WHERE process_file_name = '{path_to_file}'",
                {
                    "description_status": status,
                    "records_number": 0,
                    "control_message": "Closed",
                },
            )
            if updating[0]:
                message_type = "INFO"
                message = updating[1]["Message"]
            else:
                message_type = "CRITICAL"
                message = (
                    "status cannot be updated, Exception: " + updating[1]["Message"]
                )

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                "UPDATING STATUS OF FILE",
                message_type,
                "Updated status: " + message,
                module
            )

            


class Utils:
    """Class with other methods used in interpretation of mastercard file"""
    def __init__(self):
        self.structured = os.getenv("STRUCTURED_BUCKET")
        pass

    def generate_parquet_for_mc(self, parent_path:str,date:str,from_file: str,file_type: str, hash: str, client: str,status:str, log_name:str):
        """Generates a parquet file from mc csv base file
        
        Args:
            parent_path (str): parent path.
            date (str): file header.
            from_file (str): path to csv file.
            file_type (str): type of file.
            hash (str): hash code of file.
            client (str): client code.
            status (str): status of file.
            log_name (str): log file name.
        
        Returns:
            dict: dicionary with file data
        """
        result_path =  f"{parent_path}/RESULT/MASTERCARD/{file_type}" 

        step = "JOIN CSV FILES AND UPLOAD .PARQUET TO STRUCTURED BUCKET"
        module = "INTERPRETATION"
        s3_to = ""
        if status == "REVISION":
            result_path+="/REVISION" 
            s3_to = "REVISION/"

        df = pd.read_csv(from_file, sep="|", encoding="Latin-1", dtype=str)

        parquet_name = f"""{date}_{hash}_{file_type}.parquet"""
        local_route = f"""{result_path}/{date}/{parquet_name}"""
        pathlib.Path(f"{result_path}/{date}/").mkdir(parents=True, exist_ok=True)
        df.to_parquet(local_route)

        s3_route = f"""{s3_to}{client}/{date}/{parquet_name}"""
        upload = s3().upload_object(
            self.structured, local_route, s3_route
        )
        if upload:
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                "FILE UPLOADED TO ROUTE: " + s3_route,
                module
            )
        return  {"type": file_type, "s3": s3_route, "local": local_route,'file_header': date,'hash':hash}
