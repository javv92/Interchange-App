import os
import pathlib
from typing import List
import zipfile
import Module.Logs.logs as log
import Module.Persistence.connection as con
import hashlib as hlib
import re
import shutil
from datetime import date, datetime
from dotenv import load_dotenv


class get_files:
    """Class to get and process obtained objects from S3 buckets for interpretation."""
    def __init__(self):
        load_dotenv()
        self.sql = con.connect_to_postgreSQL()
        self.s3 = con.connect_to_s3()
        self.landing = os.getenv("LANDING_BUCKET")
        self.log = os.getenv("LOG_BUCKET")
        self.raw = os.getenv("RAW_BUCKET")
        self.module = "GET FILES"

    def hash_file(self, filename: str) -> str:
        """Obtains the hash code of the file to identify any changes

        Args:
            filename (str) : local file path

        Returns:
            str : Hash code generated
        
        """
        hash = hlib.sha256()
        block_size = 65536
        with open(filename, "rb") as file:
            while True:
                data = file.read(block_size)
                if not data:
                    break
                hash.update(data)
        return hash.hexdigest()

    def get_clients(self, client: str = None) -> list:
        """Get client's data or a list of clients
        
        Args:
            client (str) : clients code, or None if needed.
            
        Returns:
            select (list) : gets one or several clients data.
        
        """
        try:
            if client == None:
                select = self.sql.select(
                    "CONTROL.T_CUSTOMER", "WHERE status = 'ACTIVE'"
                )
                return select
            else:
                select = self.sql.select(
                    "CONTROL.T_CUSTOMER", f"WHERE status = 'ACTIVE' and code='{client}'"
                )
                return select
        except ValueError as error:
            return error

    def get_files_from_s3(self, client: str, log_name: str, file: str) -> dict:
        """Get files from client's S3 repository
        
        Args:
            client (str) : clients code.
            log_name (str): log file name.
            file (str): file name.
            
        Returns:
            dict : generated file paths data .
        
        """
        date_of_extract = datetime.now()
        formated_date = (
            str(date_of_extract.year)
            + str(date_of_extract.month)
            + str(date_of_extract.day)
            + "_"
            + str(int(datetime.timestamp(date_of_extract)))
        )
        path_to_file = "FILES/" + client + "/" + formated_date
        shutil.rmtree(path_to_file, True)
        pathlib.Path(path_to_file).mkdir(parents=True, exist_ok=True)
        customer_files = self.s3.list_content(self.landing, client + "/" + file)
        for file_obj in customer_files:
            get_zip_name = pathlib.Path(file_obj).name
            if file_obj != (client + "/"):
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "INTELICA",
                    log_name,
                    "GETTING FILES FROM S3 REPOSITORY",
                    "INFO",
                    "Get file:" + get_zip_name,
                    self.module
                )
                self.s3.get_object(
                    self.landing, file_obj, path_to_file + "/" + get_zip_name
                )
                # Delete after download file
                self.s3.delete_object(self.landing, file_obj)
        listofdir = os.listdir(path_to_file)
        for k in listofdir:
            extension = pathlib.Path(k).suffix
            name = pathlib.Path(k).stem
            complete_path = path_to_file + "/" + k
            parent = ""
            if extension == ".zip":
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "INTELICA",
                    log_name,
                    "GETTING FILES FROM S3 REPOSITORY",
                    "INFO",
                    "Unzipping file: " + k,
                    self.module
                )
                new_path = path_to_file + "/" + name
                try:
                    self.unzip_nested(new_path, complete_path, path_to_file, name)
                except zipfile.BadZipFile as e:
                    log.logs().exist_file(
                        "OPERATIONAL",
                        client,
                        "INTELICA",
                        log_name,
                        "GETTING FILES FROM S3 REPOSITORY",
                        "ERROR",
                        f"EXCEPTION: {str(log.logs().print_except())}",
                        self.module
                    )
                parent = k
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "INTELICA",
                log_name,
                "GETTING FILES FROM S3 REPOSITORY",
                "INFO",
                "Start of upload of file: " + client + "/" + formated_date + "/" + k,
                self.module
            )
            self.s3.upload_object(
                self.raw, complete_path, client + "/" + formated_date + "/" + k
            )
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "INTELICA",
                log_name,
                "GETTING FILES FROM S3 REPOSITORY",
                "INFO",
                "File uploaded on route : " + client + "/" + formated_date + "/" + k,
                self.module
            )

        log.logs().exist_file(
            "OPERATIONAL",
            client,
            "INTELICA",
            log_name,
            "GETTING FILES FROM S3 REPOSITORY",
            "INFO",
            client + " files moved to repository",
            self.module
        )
        files = self.sort_files(client, log_name, path_to_file, formated_date)
        self.clean_path(path_to_file)
        return {
            "client": client,
            "path_to_files": path_to_file,
            "log_name": log_name,
            "status": "finalized",
            "list_of_files": files,
            "execution_folder": formated_date,
        }

    def sort_files(
        self, client, log_name, path_to_files: str, execution_folder: str
    ) -> list:
        """navigate and sort files to folders
        
        Args:
            client (str): client code.
            log_name (str): log name file.
            path_to_files (str): path to local files.
            execution_folder (str): parent execution folder.

        Returns:
            list_of_files (list): list of dicts with file data.
        
        """
        exclude = ["MASTERCARD", "VISA", "OTHER", "CHECK", "RESULT"]
        list_of_files = []
        for file in os.listdir(path_to_files):
            if os.path.isdir(path_to_files + "/" + file):
                if file not in exclude:
                    filename = os.fsdecode(file)
                    if filename.endswith(".zip"):
                        continue
                    else:
                        for file_sub in os.listdir(path_to_files + "/" + file):
                            clasified_file = self.sort_by_name(
                                path_to_files + "/" + file + "/" + file_sub,
                                path_to_files,
                                client,
                            )
                            list_of_files.append(clasified_file)
                            if (
                                clasified_file["filetype"] == "IN"
                                or clasified_file["filetype"] == "OUT"
                            ):
                                status = "IN PROGRESS"
                            elif (clasified_file["filetype"] == "OTHER" or clasified_file["filetype"] == "CHECK"):
                                status = "REVISION"
                                log.logs().exist_file(
                                    "OPERATIONAL",
                                    client,
                                    "INTELICA",
                                    log_name,
                                    "SORTING FILES",
                                    "WARNING",
                                    f"File {clasified_file['path']} connot be sorted properly.",
                                    self.module
                                )
                            else:
                                status = "FINISHED"
                            control_files = [
                                (
                                    clasified_file["hash"],
                                    clasified_file["brand"],
                                    client,
                                    log_name,
                                    clasified_file["path"],
                                    date.today(),
                                    date.today(),
                                    "Sorting files",
                                    status,
                                    clasified_file["filetype"],
                                    f"{file}.zip",
                                    execution_folder,
                                )
                            ]
                            con.connect_to_postgreSQL().insert_control_file(
                                control_files
                            )

            else:
                filename = os.fsdecode(file)
                if filename.endswith(".zip"):
                    continue
                else:
                    clasified_file = self.sort_by_name(
                        path_to_files + "/" + file, path_to_files, client
                    )
                    list_of_files.append(clasified_file)
                    if (
                        clasified_file["filetype"] == "IN"
                        or clasified_file["filetype"] == "OUT"
                    ):
                        status = "IN PROGRESS"
                    elif (clasified_file["filetype"] == "OTHER" or clasified_file["filetype"] == "CHECK"):
                        status = "REVISION"
                        log.logs().exist_file(
                            "OPERATIONAL",
                            client,
                            "INTELICA",
                            log_name,
                            "SORTING FILES",
                            "WARNING",
                            f"File {clasified_file['path']} connot be sorted properly.",
                            self.module
                        )
                        
                    else:
                        status = "FINISHED"
                    control_files = [
                        (
                            clasified_file["hash"],
                            clasified_file["brand"],
                            client,
                            log_name,
                            clasified_file["path"],
                            date.today(),
                            date.today(),
                            "Sorted file",
                            status,
                            clasified_file["filetype"],
                            "",
                            execution_folder,
                        )
                    ]
                    con.connect_to_postgreSQL().insert_control_file(control_files)

        return list_of_files

    def sort_by_name(self, path_file: str, path_root: str, client: str) -> dict:
        """Creates and sort by brand and file type
        
        Args:
            client (str): client code.
            path_file (str): path to file.
            path_root (str): path to root folder of execution.

        Returns:
            dict : dict with file info.
        
        """
        filename = pathlib.Path(path_file).name
        path_MCIN = path_root + "/MASTERCARD/IN"
        path_MCOUT = path_root + "/MASTERCARD/OUT"
        path_MCOTHER = path_root + "/MASTERCARD/OTHER"
        path_VIIN = path_root + "/VISA/IN"
        path_VIOUT = path_root + "/VISA/OUT"
        path_VIOTHER = path_root + "/VISA/OTHER"
        path_OTHER = path_root + "/OTHER"
        path_CHECK = path_root + "/CHECK"
        pathlib.Path(path_MCIN).mkdir(parents=True, exist_ok=True)
        pathlib.Path(path_MCOUT).mkdir(parents=True, exist_ok=True)
        pathlib.Path(path_MCOTHER).mkdir(parents=True, exist_ok=True)
        pathlib.Path(path_VIIN).mkdir(parents=True, exist_ok=True)
        pathlib.Path(path_VIOUT).mkdir(parents=True, exist_ok=True)
        pathlib.Path(path_VIOTHER).mkdir(parents=True, exist_ok=True)
        pathlib.Path(path_OTHER).mkdir(parents=True, exist_ok=True)
        pathlib.Path(path_CHECK).mkdir(parents=True, exist_ok=True)
        query = "where customer_code in('" + client + "','ALL')"
        list_regex = con.connect_to_postgreSQL().select(
            "control.t_regex", query, ["file_format", "file_type"]
        )
        filetype = "OTHER"

        regex_in_MI = []
        regex_in_MO = []
        regex_in_MCO = []
        regex_in_VI = []
        regex_in_VO = []
        regex_in_VIO = []

        for i in list_regex:

            if i["file_type"] == "MC Incoming":
                regex_in_MI.append(i["file_format"])

            elif i["file_type"] == "MC Outgoing":
                regex_in_MO.append(i["file_format"])

            elif i["file_type"] == "IAR (T067)":
                regex_in_MCO.append(i["file_format"])

            elif i["file_type"] == "VI Incoming":
                regex_in_VI.append(i["file_format"])

            elif i["file_type"] == "VI Outgoing":
                regex_in_VO.append(i["file_format"])

            elif i["file_type"] == "ARDEF (EP302)":
                regex_in_VIO.append(i["file_format"])

        matches = 0

        for i in regex_in_MI:
            pattern = re.compile(i)
            if pattern.search(filename):
                filetype = "MC/MI"
                matches += 1
                break

        for i in regex_in_MO:
            pattern = re.compile(i)
            if pattern.search(filename):
                filetype = "MC/MO"
                matches += 1
                break

        for i in regex_in_MCO:
            pattern = re.compile(i)
            if pattern.search(filename):
                filetype = "MC/MCOTHER"
                matches += 1
                break

        for i in regex_in_VI:
            pattern = re.compile(i)
            if pattern.search(filename):
                filetype = "VI/VI"
                matches += 1
                break

        for i in regex_in_VO:
            pattern = re.compile(i)
            if pattern.search(filename):
                filetype = "VI/VOUT"
                matches += 1
                break

        for i in regex_in_VIO:
            pattern = re.compile(i)
            if pattern.search(filename):
                filetype = "VI/VOTHER"
                matches += 1
                break

        if matches > 1:
            filetype = "CHECK"

        if filetype == "MC/MI":
            path_to_ordened_file = path_MCIN + "/" + filename
            os.replace(path_file, path_to_ordened_file)
            hash_of_file = self.hash_file(path_to_ordened_file)
            brand = "MC"
            filetype = "IN"

        elif filetype == "MC/MO":
            path_to_ordened_file = path_MCOUT + "/" + filename
            os.replace(path_file, path_to_ordened_file)
            hash_of_file = self.hash_file(path_to_ordened_file)
            brand = "MC"
            filetype = "OUT"

        elif filetype == "MC/MCOTHER":
            path_to_ordened_file = path_MCOTHER + "/" + filename
            os.replace(path_file, path_to_ordened_file)
            hash_of_file = self.hash_file(path_to_ordened_file)
            brand = "MC"
            filetype = "MC/OTHER"

        elif filetype == "VI/VI":
            path_to_ordened_file = path_VIIN + "/" + filename
            os.replace(path_file, path_to_ordened_file)
            hash_of_file = self.hash_file(path_to_ordened_file)
            brand = "VI"
            filetype = "IN"

        elif filetype == "VI/VOUT":
            path_to_ordened_file = path_VIOUT + "/" + filename
            os.replace(path_file, path_to_ordened_file)
            hash_of_file = self.hash_file(path_to_ordened_file)
            brand = "VI"
            filetype = "OUT"

        elif filetype == "VI/VOTHER":
            path_to_ordened_file = path_VIOTHER + "/" + filename
            os.replace(path_file, path_to_ordened_file)
            hash_of_file = self.hash_file(path_to_ordened_file)
            brand = "VI"
            filetype = "VI/OTHER"

        elif filetype == "CHECK":
            path_to_ordened_file = path_CHECK + "/" + filename
            os.replace(path_file, path_to_ordened_file)
            hash_of_file = self.hash_file(path_to_ordened_file)
            brand = ""
            filetype = "CHECK"

        else:
            path_to_ordened_file = path_OTHER + "/" + filename
            os.replace(path_file, path_to_ordened_file)
            hash_of_file = ""
            brand = ""
            filetype = "OTHER"

        return {
            "path": path_to_ordened_file,
            "filetype": filetype,
            "hash": hash_of_file,
            "brand": brand,
        }

    def clean_path(self, base_path: str) -> None:
        """Clear files from download and unzip files

        Args:
            base_path (str): path to base files.
        
        """
        exclude = ["MASTERCARD", "VISA", "OTHER", "CHECK", "RESULT"]
        for item in os.listdir(base_path):
            if item not in exclude:
                if os.path.isdir(base_path + "/" + item):
                    os.rmdir(base_path + "/" + item)
                else:
                    os.remove(base_path + "/" + item)

    def unzip_nested(
        self,
        new_path: str,
        complete_path: str,
        path_to_file: str,
        name: str,
        sub: bool = False,
        parent_name: str | None = None,
    ) -> None:
        """Unzip files and unzip nested zips into a single folder without directory structures
        
        Args:
            new_path (str): path where file is being saved
            complete_path (str): parent directory of zip file
            path_to_file (str): zipfile
            name (str): name of zip file
            sub (bool): if is sub zip, must initialize in false when is reading directly
            parent_name (str): used only if sub is True, points to parent destiny directory.
        """
        pathlib.Path(new_path).mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(complete_path) as z:
            for files in z.namelist():
                getfilename = os.path.basename(files)
                if not getfilename:
                    continue
                source = z.open(files)
                if sub:
                    target = open(
                        os.path.join(path_to_file + "/" + parent_name, getfilename),
                        "wb",
                    )
                else:
                    target = open(
                        os.path.join(path_to_file + "/" + name, getfilename), "wb"
                    )
                with source, target:
                    shutil.copyfileobj(source, target)
                filename_sub = os.fsdecode(getfilename)
                if filename_sub.endswith(".zip"):
                    name_sub = pathlib.Path(filename_sub).stem
                    if sub:
                        name = parent_name
                    self.unzip_nested(
                        new_path,
                        new_path + "/" + filename_sub,
                        path_to_file,
                        name_sub,
                        True,
                        name,
                    )
