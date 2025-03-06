import pandas as pd
import sqlalchemy
from Module.Persistence.connection import connect_to_s3 as s3
from Module.Persistence.connection import connect_to_postgreSQL as bdpostgre
import datetime
import Module.Interpretation.Mastercard.dataelements as de
import Module.Logs.logs as log
import Module.Ingest.Mastercard.getquery as getquery
import numpy as np
from sqlalchemy.exc import SQLAlchemyError


class iar_master_update:
    """Class to process iar's table update"""
    def __init__(self):
        pass

    def update_from_parquet(
        self, path: str, client: str, log_name: str = None
    ):
        """ Process that reads a parquet file and based on the information it contains, updates the iar table .
        
        Args:
            path (str): path to parquet file.
            client (str): client code.
            log_name (str): log name.
         """
        step = "UPDATING MASTERCARD IAR"
        module = "INGEST"
        try:

            table_new = "tmp_mastercard_iar_" + (client.lower())
            schem = "temporal"
            main_schema = "operational"
            main_table = "dh_mastercard_iar"
            db = bdpostgre().prepare_engine()
            db.execution_options(autocommit=False)
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                "Opening file: " + path,
                module
            )
            df = pd.read_parquet(path=path, engine="fastparquet", storage_options=None)

            if(len(df.index) == 0):
                log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "MASTERCARD",
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
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                "Total rows in file: " + str(len(df.index)),
                module
            )
            """ Getting data"""
            columns = de.Parameters().getIPMParameters()
            columns = columns["tables"]["IP0040T1"]
            columns = list(columns.keys())
            columns.append("app_id")
            columns.append("app_full_data")
            columns.append("app_processing_date")
            columns.append("app_customer_code")
            columns.append("app_hash_file")
            columns.append("app_type_file")
            columns.append("app_header_type")
            df['app_id'] = np.arange(df.shape[0])
            df = df.filter(columns, axis=1)

            df["app_date_end"] = None
            df["app_date_valid"] = pd.to_datetime(
                df["effective_timestamp"], format="%y%j%H"
            ).apply(lambda x: datetime.datetime.strftime(x, "%Y-%m-%d %H:%M:%S"))
            df["app_processing_date"] = pd.to_datetime(
                df["app_processing_date"],format = "%Y%m%d"
            ).apply(lambda x: datetime.datetime.strftime(x, "%Y-%m-%d %H:%M:%S"))
            list_of_columns = list(df.columns)
            list_of_columns = ",".join(list_of_columns)
            """SQL querys and strings"""
            query = getquery.getquery(schem,table_new,main_schema,main_table)
            """Check unique values active in table
                As in manual:
                -Any unique combination of issuing account range (low) and
                GCMS product ID generates a separate record of this type
            """
            result =bdpostgre().insert_from_dataframe(table_new,schem,df,if_exists="replace",dtype={
                "app_processing_date": sqlalchemy.TIMESTAMP,
                "app_date_valid": sqlalchemy.TIMESTAMP,
                "app_date_end": sqlalchemy.TIMESTAMP,
                "low_range": sqlalchemy.Numeric,
                "high_range": sqlalchemy.Numeric,
            })

            log.logs().exist_file(
                    "OPERATIONAL",
                    client,
                    "MASTERCARD",
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
                "MASTERCARD",
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
                    "MASTERCARD",
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
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                "Checking for older data, Updated rows : " + str(rs[1]),
                module
            )

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                "Inserting rows from temporal table to IAR table",
                module
            )
        
            rs =  bdpostgre().execute_block(query.insert_into_dh(list_of_columns))

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
                log_name,
                step,
                "INFO",
                "Inserted rows : " + str(rs),
                module
            )

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "MASTERCARD",
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
                "MASTERCARD",
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
                "MASTERCARD",
                log_name,
                step,
                "ERROR",
                f"UNHANDLED EXCEPTION: {str(log.logs().print_except())}",
                module
            )
