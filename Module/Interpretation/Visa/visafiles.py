from email import header
from fileinput import filename
from itertools import groupby
from typing import List, Dict
import os, pathlib, time
from Module.Interpretation.Visa.parameters import Parameters
import glob
from Module.Persistence.connection import (
    connect_to_postgreSQL as conn,
    connect_to_s3 as s3,
)
import Module.Logs.logs as log
import pandas as pd
from datetime import datetime
import warnings


class read_files:
    """Class for read and interpretation of visa interchange files."""

    warnings.simplefilter(action="ignore")
    read_tcr_list = Parameters().getTCRParameters()["TCR_LIST"]
    TCR_LIST = [i for i in range(len(read_tcr_list))]

    def __init__(self) -> None:
        pass

    def group_by_header_general(self, general: list) -> List:
        """general transaction header

         Args:
            general (list): list of transactions.

        Returns:
            list : processed list of headers.

        """

        def header_counter(r: str):
            """header counter in string"""
            if len(r) == 170:
                if r[5:6] == "0":
                    header_counter.ctr += 1
                if "90  " in r[0:6]:
                    header_counter.ctr += 1

            if len(r) == 168:
                if r[3:4] == "0":
                    header_counter.ctr += 1
                if "90" in r[0:4]:
                    header_counter.ctr += 1
            return header_counter.ctr

        header_counter.ctr = 0
        return groupby(general, key=header_counter)

    def valid_file_length(self, header_line):
        reference_file_length = [168, 170]
        space_characters = header_line[2:4]
        file_length = len(header_line)
        if file_length not in reference_file_length:
            return False

        if file_length == 170 and space_characters == '  ':
            return True
        elif file_length == 168:
            return True

        return False

    def read_visa_file(
        self,
        path_to_file: str,
        parent_directory: str,
        type: str,
        log_name: str,
        client: str,
        hash: str,
    ) -> dict:
        """Method to start reading Visa transaction files to the interpretation

        Args:
            path_to_file (str): path to local file.
            parent_directory (str): path to execution folder.
            type (str): type of file.
            log_name (str): name of log file.
            client (str): client name.
            hash (str): hash code of file.

        Returns:
            dict : dictionary of file data.

        """
        step = "INTERPRETATION OF FILE"
        module = "INTERPRETATION"
        log.logs().exist_file(
            "OPERATIONAL",
            client,
            "VISA",
            log_name,
            step,
            "INFO",
            "opening file: " + path_to_file,
            module,
        )

        self.path_to_file_destiny = parent_directory + "/" + "RESULT/VISA/" + type

        resultfilename = pathlib.Path(path_to_file).name.replace(".", "_")
        pathlib.Path(self.path_to_file_destiny).mkdir(parents=True, exist_ok=True)

        log.logs().exist_file(
            "OPERATIONAL",
            client,
            "VISA",
            log_name,
            step,
            "INFO",
            "Reading file: " + path_to_file,
            module,
        )

        header_date_range = [8, 13]
        tcr_position = [3, 4]
        reference_file_length = [168, 170]

        if type == "IN":
            inc = None
            on_error = False
            file_finger_print = time.time_ns()
            status = "IN PROGRESS"
            skip_character = 0
            destiny_file = ''

            try:
                with open(path_to_file, "r", encoding="Latin-1") as f:
                    first_line = f.readline()
                    header_line = first_line.replace("\n", "").replace("\r", "")
                    file_length = len(header_line)

                    if not self.valid_file_length(header_line):
                        raise Exception('File format error: Incorrect line length')

                    if file_length == reference_file_length[1]:
                        skip_character += 2

                    header_date_range_skip = list(map(lambda num: num + skip_character, header_date_range))
                    tcr_position_skip = list(map(lambda num: num + skip_character, tcr_position))
                    header_date = header_line[header_date_range_skip[0]:header_date_range_skip[1]]

                datetime_header = (
                    datetime.strptime(str(header_date), "%y%j")
                    .date()
                    .strftime("%y%m%d")
                )
                destiny_file = datetime_header
                date_for_name = datetime.strptime(destiny_file, "%y%m%d").strftime(
                    "%Y%m%d"
                )
                transactions = []
                with open(path_to_file, "r", encoding="Latin-1") as f:
                    for line in f:
                        transactions.append(line.replace("\n", "").replace("\r", ""))
                transactions_p = [
                    list(l) for k, l in self.group_by_header_general(transactions)
                ]

                date_formated_as = (
                    datetime.strptime(str(header_date), "%y%j")
                    .date()
                    .strftime("%Y-%m-%d")
                )

                check_reprocces = conn().select(
                    "CONTROL.T_CONTROL_FILE",
                    f"""WHERE file_type = '{type}' and customer = '{client}' and brand = 'VI'
                        and code = '{hash}'
                        """,
                    "count(code) as file",
                )

                if check_reprocces[0]["file"] > 1:
                    status = "REVISION"
                    log.logs().exist_file(
                        "OPERATIONAL",
                        client,
                        "VISA",
                        log_name,
                        step,
                        "WARNING",
                        "FILE IS PART OF A REVISION",
                        module,
                    )

                if status == "REVISION":
                    self.path_to_file_destiny = self.path_to_file_destiny + "/REVISION"
                pathlib.Path(self.path_to_file_destiny + "/" + destiny_file).mkdir(
                    parents=True, exist_ok=True
                )

                header_path = self.path_to_file_destiny + "/" + datetime_header + "/"
                pathlib.Path(header_path).mkdir(parents=True, exist_ok=True)

                csv_name_inc = (
                    self.path_to_file_destiny
                    + "/"
                    + destiny_file
                    + "/"
                    + destiny_file
                    + str(file_finger_print)
                    + ".csv"
                )

                with open(csv_name_inc, "w", encoding="Latin-1") as inc:
                    id_transaction = 1
                    for i in transactions_p:
                        new = []

                        if i[0][0:2] == "90":
                            tc_90_list = [""] * len(self.TCR_LIST)
                            tc_90_list[0] = i[0]
                            new = tc_90_list
                        else:
                            for b in self.TCR_LIST:
                                value = [a for a in i if int(a[tcr_position_skip[0]:tcr_position_skip[1]]) == b]
                                if len(value) != 0:
                                    new.append(value[0])
                                else:
                                    new.append("")

                        new.insert(0, str(id_transaction))
                        new.insert(1, client)
                        new.insert(2, "IN")
                        new.insert(3, str(hash))
                        new.insert(4, str(date_formated_as))

                        id_transaction = id_transaction + 1
                        r = "¦".join(new)
                        inc.write(r)
                        inc.write("\n")

                df = pd.read_csv(
                    csv_name_inc,
                    sep="¦",
                    dtype=str,
                    header=None,
                    encoding="Latin-1",
                    engine="python",
                )

                updating = conn().update(
                    "CONTROL.T_CONTROL_FILE",
                    f"WHERE process_file_name = '{path_to_file}'",
                    {
                        "description_status": status,
                        "records_number": str(len(df.index)),
                        "file_date": date_formated_as,
                        "control_message": "Interpretation",
                    },
                )

                parquet_file = Utils().generate_parquet_for_visa(
                    parent_directory,
                    date_for_name,
                    csv_name_inc,
                    type,
                    hash,
                    client,
                    status,
                    log_name,
                )

                if updating[0]:
                    message_type = "INFO"
                    message = updating[1]["Message"]
                else:
                    message_type = "CRITICAL"
                    message = (
                        "status cannot be updated, Exception: " + updating[1]["Message"]
                    )

            except KeyError as ke:
                df = pd.DataFrame()
                parquet_file = None
                on_error = True
                if inc != None:
                    inc.close()
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
                    "VISA",
                    log_name,
                    step,
                    "WARNING",
                    "Error reading file: "
                    + path_to_file
                    + " | readed rows: "
                    + str(0)
                    + " | KeyException on parameters: '"
                    + str(ke)
                    + "' key is not founded in TCR",
                    module,
                )

            except ValueError as e:
                df = pd.DataFrame()
                parquet_file = None
                on_error = True
                if inc != None:
                    inc.close()
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
                    "VISA",
                    log_name,
                    step,
                    "WARNING",
                    "Error reading file: "
                    + path_to_file
                    + " | readed rows: "
                    + str(0)
                    + " | exception reading file: "
                    + str(e),
                    module,
                )
            except Exception as e:
                df = pd.DataFrame()
                parquet_file = None
                on_error = True
                if inc != None:
                    inc.close()

                if destiny_file != '':
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
                    "VISA",
                    log_name,
                    step,
                    "WARNING",
                    "Error reading file: "
                    + path_to_file
                    + " | readed rows: "
                    + str(0)
                    + " | exception reading file: "
                    + str(e),
                    module,
                )

            if on_error:
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "VISA",
                    log_name,
                    step,
                    "ERROR",
                    "Closed reading of file with error: " + path_to_file,
                    module,
                )
                status = "ERROR"

            else:
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "VISA",
                    log_name,
                    step,
                    "INFO",
                    "Finished read of file: "
                    + path_to_file
                    + " | readed rows: "
                    + str(len(df.index)),
                    module,
                )
                if status == "IN PROGRESS":
                    status = "PROCESSED"

            updating = conn().update(
                "CONTROL.T_CONTROL_FILE",
                f"WHERE process_file_name = '{path_to_file}'",
                {
                    "description_status": status,
                    "records_number": str(len(df.index)),
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
                "VISA",
                log_name,
                "UPDATING STATUS OF FILE",
                message_type,
                "UPDATE STATUS: " + message,
                module,
            )

        if type == "OUT":
            out = None
            on_error = False
            file_finger_print = time.time_ns()
            status = "IN PROGRESS"
            skip_character = 0
            destiny_file = ''

            try:
                with open(path_to_file, "r", encoding="Latin-1") as f:
                    first_line = f.readline()
                    header_line = first_line.replace("\n", "").replace("\r", "")
                    file_length = len(header_line)

                    if not self.valid_file_length(header_line):
                        raise Exception('File format error: Incorrect line length')

                    if file_length == reference_file_length[1]:
                        skip_character += 2

                    header_date_range_skip = list(map(lambda num: num + skip_character, header_date_range))
                    tcr_position_skip = list(map(lambda num: num + skip_character, tcr_position))
                    header_date = header_line[header_date_range_skip[0]:header_date_range_skip[1]]

                datetime_header = (
                    datetime.strptime(str(header_date), "%y%j")
                    .date()
                    .strftime("%y%m%d")
                )
                destiny_file = datetime_header
                date_for_name = datetime.strptime(destiny_file, "%y%m%d").strftime(
                    "%Y%m%d"
                )
                transactions = []

                with open(path_to_file, "r", encoding="Latin-1") as f:
                    for line in f:
                        transactions.append(line.replace("\n", "").replace("\r", ""))
                transactions_p = [
                    list(l) for k, l in self.group_by_header_general(transactions)
                ]

                date_formated_as = (
                    datetime.strptime(str(header_date), "%y%j")
                    .date()
                    .strftime("%Y-%m-%d")
                )

                check_reprocces = conn().select(
                    "CONTROL.T_CONTROL_FILE",
                    f"""WHERE file_type = '{type}' and customer = '{client}' and brand = 'VI'
                        and code = '{hash}'
                        """,
                    "count(code) as file",
                )

                if check_reprocces[0]["file"] > 1:
                    status = "REVISION"
                    log.logs().exist_file(
                        "OPERATIONAL",
                        client,
                        "VISA",
                        log_name,
                        step,
                        "WARNING",
                        "FILE IS PART OF A REVISION",
                        module,
                    )

                if status == "REVISION":
                    self.path_to_file_destiny = self.path_to_file_destiny + "/REVISION"
                pathlib.Path(self.path_to_file_destiny + "/" + destiny_file).mkdir(
                    parents=True, exist_ok=True
                )

                header_path = self.path_to_file_destiny + "/" + datetime_header + "/"
                pathlib.Path(header_path).mkdir(parents=True, exist_ok=True)

                csv_name_out = (
                    self.path_to_file_destiny
                    + "/"
                    + destiny_file
                    + "/"
                    + destiny_file
                    + str(file_finger_print)
                    + ".csv"
                )

                with open(csv_name_out, "w", encoding="Latin-1") as out:
                    id_transaction = 1
                    for i in transactions_p:
                        new = []

                        if i[0][0:2] == "90":
                            tc_90_list = [""] * len(self.TCR_LIST)
                            tc_90_list[0] = i[0]
                            new = tc_90_list
                        else:
                            for b in self.TCR_LIST:
                                value = [a for a in i if int(a[tcr_position_skip[0]:tcr_position_skip[1]].replace(' ','0')) == b]
                                if len(value) != 0:
                                    new.append(value[0])
                                else:
                                    new.append("")

                        new.insert(0, str(id_transaction))
                        new.insert(1, client)
                        new.insert(2, "OUT")
                        new.insert(3, str(hash))
                        new.insert(4, str(date_formated_as))
                        id_transaction = id_transaction + 1
                        r = "¦".join(new)
                        out.write(r)
                        out.write("\n")

                df = pd.read_csv(
                    csv_name_out,
                    sep="¦",
                    dtype=str,
                    encoding="Latin-1",
                    header=None,
                    engine="python",
                )

                updating = conn().update(
                    "CONTROL.T_CONTROL_FILE",
                    f"WHERE process_file_name = '{path_to_file}'",
                    {
                        "description_status": status,
                        "records_number": str(len(df.index)),
                        "file_date": date_formated_as,
                        "control_message": "Interpretation",
                    },
                )

                parquet_file = Utils().generate_parquet_for_visa(
                    parent_directory,
                    date_for_name,
                    csv_name_out,
                    type,
                    hash,
                    client,
                    status,
                    log_name,
                )

                if updating[0]:
                    message_type = "INFO"
                    message = updating[1]["Message"]
                else:
                    message_type = "CRITICAL"
                    message = (
                        "status cannot be updated, Exception: " + updating[1]["Message"]
                    )

            except KeyError as ke:
                df = pd.DataFrame()
                parquet_file = None
                on_error = True
                if out != None:
                    out.close()
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
                    "VISA",
                    log_name,
                    step,
                    "WARNING",
                    "Error reading file: "
                    + path_to_file
                    + " | readed rows: "
                    + str(0)
                    + " | KeyException on parameters: '"
                    + str(ke)
                    + "' key is not founded in TCR",
                    module,
                )

            except ValueError as e:
                df = pd.DataFrame()
                parquet_file = None
                on_error = True
                if out != None:
                    out.close()
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
                    "VISA",
                    log_name,
                    step,
                    "WARNING",
                    "Error reading file: "
                    + path_to_file
                    + " | readed rows: "
                    + str(0)
                    + " | exception reading file: "
                    + str(e),
                    module,
                )
            except Exception as e:
                df = pd.DataFrame()
                parquet_file = None
                on_error = True
                if out != None:
                    out.close()

                if destiny_file != '':
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
                    "VISA",
                    log_name,
                    step,
                    "WARNING",
                    "Error reading file: "
                    + path_to_file
                    + " | readed rows: "
                    + str(0)
                    + " | exception reading file: "
                    + str(e),
                    module,
                )

            if on_error:
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "VISA",
                    log_name,
                    step,
                    "ERROR",
                    "Closed reading of file with error: " + path_to_file,
                    module,
                )
                status = "ERROR"

            else:
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "VISA",
                    log_name,
                    step,
                    "INFO",
                    "Finished read of file: "
                    + path_to_file
                    + " | readed rows: "
                    + str(len(df.index)),
                    module,
                )
                if status == "IN PROGRESS":
                    status = "PROCESSED"

            updating = conn().update(
                "CONTROL.T_CONTROL_FILE",
                f"WHERE process_file_name = '{path_to_file}'",
                {
                    "description_status": status,
                    "records_number": str(len(df.index)),
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
                "VISA",
                log_name,
                "UPDATING STATUS OF FILE",
                message_type,
                "UPDATE STATUS: " + message,
                module,
            )

        return {
            "result": True,
            "message": "Finished",
            "on_error": on_error,
            "parquet_info": parquet_file,
            "status": status,
        }

    def read_visa_ardef(
        self,
        path_to_file: str,
        parent_directory: str,
        type: str,
        log_name: str,
        client: str,
        hash: str,
    ) -> dict:
        """Method to start reading Visa ARDEF files to the interpretation

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
        st_timestamp = int(float(datetime.now().timestamp()))
        on_error = False
        step = "INTERPRETATION OF ARDEF FILE"
        module = "INTERPRETATION"

        log.logs().exist_file(
            "OPERATIONAL",
            client,
            "VISA",
            log_name,
            step,
            "INFO",
            "opening file: " + path_to_file,
            module,
        )

        self.path_to_file_destiny = parent_directory + "/" + "RESULT/VISA/" + "ARDEF"
        resultfilename = pathlib.Path(path_to_file).name.replace(".", "_")
        pathlib.Path(self.path_to_file_destiny).mkdir(parents=True, exist_ok=True)

        log.logs().exist_file(
            "OPERATIONAL",
            client,
            "VISA",
            log_name,
            step,
            "INFO",
            "Reading file: " + path_to_file,
            module,
        )

        try:
            ardef = None
            file_finger_print = time.time_ns()
            status = "IN PROGRESS"
            date_formated_as = datetime.now().strftime("%Y-%m-%d")

            lines = []
            versions = []
            with open(path_to_file, "r", encoding="Latin-1") as f:
                for line in f:
                    if line[0:2] == "VL" and "C****" not in line:
                        lines.append(line.replace("\n", "").replace("\r", ""))

                    if line[0:8] == "AAACTRNG" and line[10:17] == "AEPACRN":
                        header_date = line[23:31]
                        version_number = line[63:67]
                        versions.append([version_number, header_date])

            ultimate_header = [
                version
                for version in versions
                if version[0] == max([version[0] for version in versions])
            ]
            ultimate_date = ultimate_header[0][1]
            date_formated_as = (
                datetime.strptime(str(ultimate_date), "%Y%m%d")
                .date()
                .strftime("%Y-%m-%d")
            )
            destiny_file = (
                datetime.strptime(str(ultimate_date), "%Y%m%d")
                .date()
                .strftime("%y%m%d")
            )

            ultimate_version = ultimate_header[0][0]
            date_for_name = datetime.strptime(destiny_file, "%y%m%d").strftime("%Y%m%d")
            destiny_directory = parent_directory + "/RESULT/VISA/ARDEF/" + destiny_file
            pathlib.Path(destiny_directory).mkdir(parents=True, exist_ok=True)

            df = pd.DataFrame(lines)
            df.insert(0, "app_file_header", date_formated_as)
            df.insert(1, "app_hash_file", hash)

            df.to_csv(
                destiny_directory
                + "/"
                + destiny_file
                + "_"
                + ultimate_version
                + ".csv",
                index=None,
                header=None,
                sep="¦",
            )

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                "INFO",
                f"Finished reading of file {path_to_file}",
                module,
            )
            status = "PROCESSED"
            updating = conn().update(
                "CONTROL.T_CONTROL_FILE",
                f"WHERE process_file_name = '{path_to_file}'",
                {
                    "description_status": status,
                    "records_number": str(len(df.index)),
                    "file_date": date_formated_as,
                    "control_message": "Interpretation",
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

            step = "UPDATING STATUS OF FILE"
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                message_type,
                "Updated status: " + message,
                module,
            )

            csv_name = (
                destiny_directory + "/" + destiny_file + "_" + ultimate_version + ".csv"
            )
            parquet_file = Utils().generate_parquet_for_visa(
                parent_directory,
                date_for_name,
                csv_name,
                "ARDEF",
                hash,
                client,
                status,
                log_name,
            )

            return {
                "result": True,
                "message": "Finished",
                "on_error": on_error,
                "parquet_info": parquet_file,
                "status": status,
            }

        except Exception as e:

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                "INTERPRETATION OF ARDEF FILE",
                "WARNING",
                "Error reading file: "
                + path_to_file
                + " | readed rows: "
                + str(0)
                + " | exception reading file: "
                + str(e)
                + " | invalid file format",
                module,
            )

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                "INTERPRETATION OF ARDEF FILE",
                "ERROR",
                "Closed reading of file with error: " + path_to_file,
                module,
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
                "VISA",
                log_name,
                "UPDATING STATUS OF FILE",
                message_type,
                "Updated status: " + message,
                module,
            )


class Utils:
    """Class with other methods used in interpretation of visa file"""

    read_tcr_list = Parameters().getTCRParameters()["TCR_LIST"]
    TCR_LIST = [i for i in range(len(read_tcr_list))]

    def __init__(self) -> None:
        self.structured = os.getenv("STRUCTURED_BUCKET")
        pass

    def generate_parquet_for_visa(
        self,
        parent_path: str,
        date: str,
        from_file: str,
        file_type: str,
        hash: str,
        client: str,
        status: str,
        log_name,
    ):
        """Generates a parquet file from visa csv base file

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
        result_path = f"{parent_path}/RESULT/VISA/{file_type}"

        step = "JOIN CSV FILES AND UPLOAD .PARQUET TO STRUCTURED BUCKET"
        module = "INTERPRETATION"
        s3_to = ""
        if status == "REVISION":
            result_path += "/REVISION"
            s3_to = "REVISION/"

        df = pd.read_csv(from_file, header=None, sep="¦", encoding="Latin-1", dtype=str)
        df.applymap(str)

        if file_type == "IN" or file_type == "OUT":
            column_quantity = len(self.TCR_LIST) + 5
            df.columns = [str(i) for i in range(column_quantity)]

        else:
            df.columns = ["0", "1", "2"]
        parquet_name = f"""{date}_{hash}_{file_type}.parquet"""
        local_route = f"""{result_path}/{date}/{parquet_name}"""
        pathlib.Path(f"{result_path}/{date}/").mkdir(parents=True, exist_ok=True)
        df.to_parquet(local_route)

        s3_route = f"""{s3_to}{client}/{date}/{parquet_name}"""
        upload = s3().upload_object(self.structured, local_route, s3_route)
        if upload:
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                "INFO",
                "FILE UPLOADED TO ROUTE: " + s3_route,
                module,
            )
        return {
            "type": file_type,
            "s3": s3_route,
            "local": local_route,
            "file_header": date,
            "hash": hash,
        }
