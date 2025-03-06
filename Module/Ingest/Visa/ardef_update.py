from typing import List, Dict
import pandas as pd
from datetime import datetime
from Module.Interpretation.Visa.parameters import Parameters
from Module.Persistence.connection import connect_to_postgreSQL as bdpostgre
import Module.Ingest.Visa.getquery as getquery
import sqlalchemy
import Module.Logs.logs as log
import numpy as np

class ardef_master_update:
    """Class to process Ardef's table update"""

    def __init__(self) -> None:
        pass

    def update_from_parquet(
        self,
        path: str,
        client: str = None,
        log_name: str = None
    ):
        """Process that reads a parquet file and based on the information it contains, updates the ARDEF table in the database.
        
        Args:
            path (str): path to parquet file.
            client (str): client code.
            log_name (str): log name.
        
        """
        try:

            module = "INGEST"
            ardef_parameters = Parameters().getARDEFParameters()["tables"]["ARDEF"]
            db = bdpostgre().prepare_engine()
            db.execution_options(autocommit=False)
            table_new = f"tmp_visa_ardef_{client.lower()}"
            main_schema = "operational"
            main_table = "dh_visa_ardef"
            schem = "temporal"
            step = "UPDATING VISA ARDEF"
              
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                "INFO",
                "Opening file: " + path,
                module,
            )
            df = pd.read_parquet(path=path, engine="fastparquet", storage_options=None)

            if(len(df.index) == 0):
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "VISA",
                    log_name,
                    step,
                    "INFO",
                    "No rows found in file",
                    module
                )
                return

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                "INFO",
                "Total rows in file: " + str(len(df.index)),
                module,
            )
            df.columns = ["app_file_header", "app_hash_file", "app_full_data"]
            df["app_date_end"] = None
            df["app_file_header"] = df["app_file_header"].str.replace("Â", "")
            df["app_hash_file"] = df["app_hash_file"].str.replace("Â", "")

            for i in ardef_parameters.items():
                a = int(i[1]["start"])
                b = int(i[1]["end"])
                c = str(i[0])
                df[str(c)] = df["app_full_data"].map(lambda x: x[a:b])

            df["app_date_valid"] = [
                datetime.strptime(i, "%Y%m%d").date().strftime("%Y-%m-%d")
                for i in df["effective_date"]
            ]
            df["app_processing_date"] = [
            datetime.strptime(i, "%Y%m%d").date().strftime("%Y-%m-%d")
            for i in df["effective_date"]
            ]        
            df.index = np.arange(1, len(df) + 1)
            df["app_id"] = df.index
            df['product_id'] = df['product_id'].str.replace(' ','')
            df['app_type_file'] = 'ARDEF'
            df['app_customer_code'] = str(client)
            
            list_of_columns = list(df.columns)
            list_of_columns = ",".join(list_of_columns)
            
            query = getquery.getquery(schem,table_new,main_schema,main_table)

            result = bdpostgre().insert_from_dataframe(table_new,schem,df,if_exists="replace",dtype={
                    "app_date_end": sqlalchemy.DateTime,
                    "app_file_header": sqlalchemy.DateTime,
                    "app_date_valid": sqlalchemy.DateTime,
                    "app_processing_date" : sqlalchemy.DateTime
                }
            )

            log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "VISA",
                    log_name,
                    step,
                    "INFO",
                    "Created and inserted data in table "
                    + schem
                    + "."
                    + table_new,
                    module
                )
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                "INFO",
                "Updating from temporal table "
                + schem
                + "."
                + table_new,
                module
            )
            rs = bdpostgre().execute_block(query.up_from_temp(),True)
            log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "VISA",
                    log_name,
                    step,
                    "INFO",
                    "Updated rows : " + str(rs[1]),
                    module
                )
            
            rs = bdpostgre().execute_block(query.up_temp_from_dh(),True)

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                "INFO",
                "Checking for older data, Updated rows : " + str(rs[1]),
                module
            )

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                "INFO",
                "Inserting rows from temporal table to ARDEF table",
                module
            )
        
            rs =  bdpostgre().execute_block(query.insert_into_dh(list_of_columns))

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                "INFO",
                "Inserted rows : " + str(rs),
                module
            )

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                "INFO",
                "Droping temporal table",
                module
            )

            result = bdpostgre().drop_table(f"{schem}.{table_new}")
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                step,
                "INFO",
                result,
                module
            )

        except Exception as e:
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA",
                log_name,
                "Interpretation of VISA file",
                "ERROR",
                "Closing file, Error while updating table: " + str(e),
                module,
            )
