from time import sleep
import calendar
import os
import pathlib
import shutil
import sys
from datetime import date, datetime, timedelta
from operator import index

import numpy as np
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

import Module.Adapter.adapters as adapters
import Module.Logs.logs as log
import Module.Persistence.connection as connection
import Module.SchemeFee.getquery as getquery

class scheme_fee:
    def __init__(self):
        load_dotenv()
        pass

    def generate_table(self, client: str, date: str):
        """Generates a csv on the scheme fee's bucket in the clients 'out' directory with feeded date data

        Args:
            client (str): must exists in database.
            date (str): must be in format YYYYMM
        """
        try:
            s3 = connection.connect_to_s3()
            module = "OPERATIONAL"
            exec_module = "SCHEME FEE"

            step =  ""
            temporal_schema = "temporal"
            log_name = log.logs().new_log(
                module, "", client, "GENERATION OF SCHEME FEE REPORT", "SCHEME_FEE",exec_module
            )

            date = str(date)
            year = date[0:4]
            month = date[4:6]
            date_month = calendar.monthrange(int(year),int(month))

            max_null = int(os.getenv("SCHEME_FEE_TOLERANCE",5))

            date_of_extract = datetime.now()
            formated_date = (
                str(date_of_extract.year)
                + str(date_of_extract.month)
                + str(date_of_extract.day)
                + "_"
                + str(int(datetime.timestamp(date_of_extract)))
            )
            
            first_day = datetime.strptime(f"{year}-{month}-01 00:00:00","%Y-%m-%d %H:%M:%S")
            last_day = datetime.strptime(f"{year}-{month}-{date_month[1]} 23:59:59","%Y-%m-%d %H:%M:%S")

            month_of_report = datetime.strptime(month, "%m")
            month_name = date

            go_main = adapters.get_others(client,log_name)
            begin_date = datetime.strftime(first_day,"%Y%m%d")
            form_date = datetime.strftime(last_day,"%Y%m%d")
            exists_month = connection.connect_to_postgreSQL().select(table="operational.mh_scheme_fee_sumary",conditions= f"where report_month = '{date}' and report_client_code = '{client}'")
            previous = False
            if len(exists_month)> 0:
                loop = True
                previous = True
                while loop:
                    loop = self.input_validate(client,module,log_name,step,month_name,exec_module)
            go_main.config_additional_table(begin_date,form_date,'mh_transaction_scheme_fee')
            go_main.config_additional_table(begin_date,begin_date,config_table_name='mh_monthly_scheme_fee')

                    
            filename = f"{client}_{date}_{formated_date}.csv"
            s3_bucket =  os.getenv("SCHEME_FEE_BUCKET")
            s3_route = f"OUT/{client}"
            temp_route_folder = f"FILES/SCHEME_FEE/OUT/{client}/{date}"
            temp_file_name = f"{temp_route_folder}/{filename}"
            temp_transaction_table = f"temp_scheme_fee_transaction_{client.lower()}_{date}"
            temp_report_table = f"temp_scheme_fee_report_{client.lower()}_{date}"
            temp_report_table_legacy = f"temp_scheme_fee_report_legacy_{client.lower()}_{date}"

            pathlib.Path(temp_route_folder).mkdir(parents=True, exist_ok=True)

            db = connection.connect_to_postgreSQL()

            querys = getquery.getquery(client)
            columns = querys.get_detail_columns()

            
            ##report_legacy_columns = querys.get_report_legacy_columns()
            report_legacy_columns_filter = querys.get_report_legacy_columns_filter()

            create_temporal_transactions = querys.temp_table_scheme_fee_transaction()
            create_temporal_report = querys.temp_table_scheme_fee_report()
            create_temporal_report_legacy = querys.temp_table_scheme_fee_report_legacy()
            insert_into_query = querys.get_insert_detail(columns,temporal_schema,temp_transaction_table)
            begin_date = first_day.strftime("%Y-%m-%d")
            form_date = last_day.strftime("%Y-%m-%d")  
            begin_date_partition = first_day.strftime("%Y%m%d")
            last_date_partition = last_day.strftime("%Y%m%d") 


            visa_issuer_query = querys.get_issuers_visa(formated_date,date,begin_date,form_date,insert_into_query)
            visa_acquirer_query = querys.get_acquirer_visa(formated_date,date,begin_date,form_date,insert_into_query)
            visa_sms_query = querys.get_sms_visa(formated_date,date,begin_date,form_date,insert_into_query)
            visa_on_us_query = querys.get_on_us_visa(formated_date,date,begin_date,form_date,insert_into_query)
            visa_sms_on_us_query = querys.get_sms_on_us_visa(formated_date,date,begin_date,form_date,insert_into_query)

            master_card_gen = querys.get_transactions_mastercard(formated_date,date,begin_date,form_date,insert_into_query)
            mastercard_on_us_query = querys.get_on_us_mastercard(formated_date,date,begin_date,form_date,insert_into_query)

            visa_duplicate_on_us_flag = db.select("control.t_customer",f"where code = '{client}'","duplicate_on_us_flag_visa")[0]['duplicate_on_us_flag_visa']
            mastercard_duplicate_on_us_flag = db.select("control.t_customer",f"where code = '{client}'","duplicate_on_us_flag_mastercard")[0]['duplicate_on_us_flag_mastercard']

            calculate_report_currency = querys.get_exchange_rate_calculation(temporal_schema,temp_transaction_table,first_day,last_day)    
            calculating_switch_code =querys.update_switch_codes(temporal_schema,temp_transaction_table,begin_date,form_date)   
            calculating_ticket_size =querys.update_size_tickets(temporal_schema,temp_transaction_table,begin_date,form_date)   


            inserting_data_to_rpt_table = querys.get_insert_into_report_table(temporal_schema,temp_report_table,temporal_schema,temp_transaction_table,formated_date,month_name,first_day,last_day)
            updating_temp_report = querys.get_report_extra_columns(temporal_schema,temp_report_table,'',first_day)
            ##get_report_legacy = querys.get_report_legacy_query(temporal_schema,temp_report_table_legacy,report_legacy_columns,formated_date)
            get_report_legacy_filter = querys.get_report_legacy_query(temporal_schema,temp_report_table_legacy,report_legacy_columns_filter,formated_date)

            inserting_data_to_rpt_table_legacy = querys.get_insert_into_report_legacy_table(temporal_schema,temp_report_table_legacy,temporal_schema,temp_report_table,formated_date,month_name,first_day,last_day)

            delete_old_detail = querys.get_delete_detail(date,formated_date)
            delete_old_report = querys.get_delete_report(month_name,formated_date)
            delete_old_sumary = querys.get_delete_sumary(month_name,formated_date)
            delete_old_report_legacy = querys.get_delete_report_legacy(month_name,formated_date)

            column_base_scheme_fee = [{'column_name': 'app_type_file', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'app_customer_code', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'app_execution_id', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'scheme_fee_execution_month', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'app_id','length': '10', 'column_type': 'numeric'}
                            ,{'column_name': 'app_hash_file', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'table_description', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'app_processing_date','length': '10', 'column_type': 'date'}
                            ,{'column_name': 'account_number', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'card_acceptor_id', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'account_funding_source', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'account_range_country', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'product_id', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'range_program_id', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'jurisdiction', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'business_transaction_type_id', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'reversal_indicator', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'currency_local_indicator', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'motoec_indicator', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'transaction_count','length': '10', 'column_type': 'numeric'}
                            ,{'column_name': 'settlement_amount','length': '10', 'column_type': 'numeric'}
                            ,{'column_name': 'transaction_scheme_fee_cost','length': '10', 'column_type': 'numeric'}
                            ,{'column_name': 'transaction_brand', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'merchant_country_code', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'switch_code', 'length': '4', 'column_type': 'text'}
                            ,{'column_name': 'settlement_currency', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'bank_country', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'business_mode_id', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'size_ticket', 'length': '50', 'column_type': 'text'}
                            ,{'column_name': 'unitary_scheme_fee_cost','length': '10', 'column_type': 'numeric'}
                            ,{'column_name': 'estimated_scheme_fee_cost','length': '10', 'column_type': 'numeric'}
                            ,{'column_name': 'unitary_estimated_scheme_fee_cost','length': '10', 'column_type': 'numeric'}
                            ,{'column_name': 'transaction_purchase_date','length': '10', 'column_type': 'timestamp'}
                            ,{'column_name': 'exchange_rate','length': '10', 'column_type': 'numeric'}
                            ,{'column_name': 'report_amount','length': '10', 'column_type': 'numeric'}
                            ,{'column_name': 'report_currency', 'length': '50', 'column_type': 'text'}]
            
            
            columns_scheme_fee = []
            columns_scheme_fee.extend(column_base_scheme_fee)
            
            step = "PREPARING DATA FOR REPORT"
        
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Drop temporal tables if exists",
                exec_module
            )
            result = db.drop_table(f"{temporal_schema}.{temp_transaction_table}")
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )
            result = db.drop_table(f"{temporal_schema}.{temp_report_table}")
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )
            result = db.drop_table(f"{temporal_schema}.{temp_report_table_legacy}")
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Creating temporal tables for report",
                exec_module
            )

            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Creating temporal transaction table with partitions",
                exec_module
            )
            
            result = db.create_table(columns_scheme_fee,temporal_schema+'.'+temp_transaction_table,True,'app_processing_date','range')
            result = db.create_table_partition_range(table_scheme=temporal_schema,
                                                     table_name=temp_transaction_table,
                                                     partition_scheme=temporal_schema,
                                                     partition_name=temp_transaction_table,
                                                     partition_start_value=begin_date_partition,
                                                     partition_end_value=last_date_partition)
            
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )
            
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Creating temporal tables for report",
                exec_module
            )
            
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )
            result = db.create_table_from_select(create_temporal_report,f"{temporal_schema}.{temp_report_table}")
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )
            result = db.create_table_from_select(create_temporal_report_legacy,f"{temporal_schema}.{temp_report_table_legacy}")
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Inserting data into temporal table {temporal_schema}.{temp_transaction_table}",
                exec_module
            )
            current_time = datetime.now().time()
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Insert process started at {current_time}",
                exec_module
            )
            result = db.execute_block(visa_issuer_query)
            current_time = datetime.now().time()
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Insert Visa ISS completed at {current_time}",
                exec_module
            )
            result = db.execute_block(visa_acquirer_query)
            current_time = datetime.now().time()
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Insert Visa ACQ completed at {current_time}",
                exec_module
            )
            result = db.execute_block(visa_sms_query)
            current_time = datetime.now().time()
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Insert Visa SMS completed at {current_time}",
                exec_module
            )
            result = db.execute_block(master_card_gen)
            current_time = datetime.now().time()
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Insert MasterCard completed at {current_time}",
                exec_module
            )

            if visa_duplicate_on_us_flag:
                result = db.execute_block(visa_on_us_query)
                current_time = datetime.now().time()
                log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Insert VISA ACQ ONUS completed at {current_time}",
                exec_module
                )
                result = db.execute_block(visa_sms_on_us_query)
                current_time = datetime.now().time()
                log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Insert VISA SMS ONUS completed at {current_time}",
                exec_module
            )
            if mastercard_duplicate_on_us_flag:       
                result = db.execute_block(mastercard_on_us_query)
                current_time = datetime.now().time()
                log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Insert MASTERCARD ACQ ONUS completed at {current_time}",
                exec_module
            )
            table_count = db.table_count(
                temporal_schema,
                temp_transaction_table
            )    
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Total rows inserted : {table_count}",
                exec_module
            )
            current_time = datetime.now().time()
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Insert process completed at {current_time}",
                exec_module
            )
            # log.logs().exist_file(
            #     module,
            #     client,
            #     "VISA AND MASTERCARD",
            #     log_name,
            #     step,
            #     "INFO",
            #     "Calculating report exchange rates and amounts",
            #     exec_module
            # )
            # result = db.execute_block(calculate_report_currency)
            # log.logs().exist_file(
            #     module,
            #     client,
            #     "VISA AND MASTERCARD",
            #     log_name,
            #     step,
            #     "INFO",
            #     result,
            #     exec_module
            # )

            
            # log.logs().exist_file(
            #     module,
            #     client,
            #     "VISA AND MASTERCARD",
            #     log_name,
            #     step,
            #     "INFO",
            #     "Check for null validation columns",
            #     exec_module
            # )
            # result = db.select(f"{temporal_schema}.{temp_transaction_table}", cols="count(app_id) quantity")
            # null_rows = int(result[0]["quantity"])
            # if null_rows > 0:
            #     if null_rows == int(max_null):
            #         log.logs().exist_file(
            #             module,
            #             client,
            #             "VISA AND MASTERCARD",
            #             log_name,
            #             step,
            #             "CRITICAL",
            #             "Max null tolerance reached, aborting process.",
            #             exec_module
            #         )
            #         exit()
            #     else:
            #         log.logs().exist_file(
            #             module,
            #             client,
            #             "VISA AND MASTERCARD",
            #             log_name,
            #             step,
            #             "WARNING",
            #             f"{null_rows} rows with validation fields in null are going to be removed and not included in the report.",
            #             exec_module
            #         )

            current_time = datetime.now().time() 
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Switch code update started at {current_time}",
                exec_module
            )   
            result = db.execute_block(calculating_switch_code)
            current_time = datetime.now().time()
            sleep(1)
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Switch code update completed at {current_time}",
                exec_module
            )
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Calculating ticket size for transactions",
                exec_module
            )
            current_time = datetime.now().time() 
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Size ticket update started at {current_time}",
                exec_module
            )   
            result = db.execute_block(calculating_ticket_size)
            current_time = datetime.now().time()
            sleep(1)
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Size ticket update completed at {current_time}",
                exec_module
            )
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Grouping data and inserting in temporal report table",
                exec_module
            )
            result = db.execute_block(inserting_data_to_rpt_table)            
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Updating internal columns of temporal table",
                exec_module
            )
            result = db.execute_block(updating_temp_report)
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )

            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Inserting data in scheme fee legacy report table",
                exec_module
            )
            result = db.execute_block(inserting_data_to_rpt_table_legacy)
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )
            
            step = "CREATING CSV SCHEME FEE REPORT"
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Creating csv report",
                exec_module
            )
            df = db.select_to_df_object(get_report_legacy_filter)
            df = df.filter(report_legacy_columns_filter, axis=1)            
        
            numeric_excluded_columns = ['app_id','set_mth','txn_cnt','txn_amt', 'txn_sfc','unt_sfc', 'est_sch_fee_amt','unt_est_sch_fee_amt']
            numeric_columns = [col for col in df.select_dtypes(include=[np.number]).columns if col not in numeric_excluded_columns]
            df[numeric_columns] = df[numeric_columns].fillna(255)

            fill_with_zero_columns = ['txn_cnt','txn_amt', 'txn_sfc','unt_sfc', 'est_sch_fee_amt','unt_est_sch_fee_amt']
            df[fill_with_zero_columns] = df[fill_with_zero_columns].fillna(0)
            
            text_excluded_columns = ['app_execution_id','rpt_bnk_id']
            text_columns = [col for col in df.select_dtypes(include=['object']).columns if col not in text_excluded_columns]
            df[text_columns] = df[text_columns].fillna('9999999999999999')

            format ={
                     'app_id'               : int,
                     'app_execution_id'     : str,
                     'rpt_bnk_id'           : str,
                     'set_mth'              : int,
                     'bus_id'               : int,
                     'sch_id'               : int,
                     'tkt_siz_id'           : int,
                     'prd_id'               : int,
                     'prg_id'               : int,
                     'fnd_src_id'           : int,
                     'txn_scp_id'           : int,
                     'txn_typ_id'           : int,
                     'txn_rvsl_flg_id'      : int,
                     'txn_crncy_lcl_flg_id' : int,
                     'txn_crd_prs_flg_id'   : int,
                     'mct_cd'               : str,
                     'swt_cd'               : str,
                     'txn_cnt'              : int,
                     'txn_amt'              : float,
                     'txn_sfc'              : float,
                     'mct_ctry_id'          : int,
                     'unt_sfc'              : float,
                     'est_sch_fee_amt'      : float,
                     'unt_est_sch_fee_amt'  : float
                     }  
            df = df.astype(format, errors='ignore')
            df[numeric_columns] = df[numeric_columns].apply(lambda x: pd.to_numeric(x, errors='coerce'))
            df.to_csv(temp_file_name, sep=",", index=False)
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Exporting report to s3 bucket",
                exec_module
            )
            s3.upload_object(s3_bucket,temp_file_name,f"{s3_route}/{filename}")
            
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Report uploaded in S3 bucket. File route: {s3_route}/{filename}",
                exec_module
            )
            
            step = "CLEANING AND FINISHING PROCESS"
            total_transactions = db.select(f"{temporal_schema}.{temp_transaction_table}",cols="count(app_id) quantity")
            insert_into_sumary = querys.get_insert_into_sumary(formated_date,month_name,total_transactions[0]["quantity"],len(df),s3_route)
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Saving report sumary into database, Month {month_name}",
                exec_module
            )
            result = db.execute_block(insert_into_sumary)

            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Inserting data in scheme fee transaction table",
                exec_module
            )
            result = db.insert_from_table(temporal_schema,temp_transaction_table,"operational","mh_transaction_scheme_fee")
            log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    result,
                    exec_module
                )
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Inserting data in scheme fee report table",
                exec_module
            )
            result = db.insert_from_table(temporal_schema,temp_report_table,"operational","mh_monthly_scheme_fee")
            log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    result,
                    exec_module
                )
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Inserting data in scheme fee legacy report table",
                exec_module
            )
            result = db.insert_from_table(temporal_schema,temp_report_table_legacy,"operational","mh_monthly_scheme_fee_legacy")
            log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    result,
                    exec_module
                )
            if previous:
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    "Deleting old sumary already generated",
                    exec_module
                )
                result = db.execute_block(delete_old_sumary)
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    result,
                    exec_module
                )
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    "Deleting old generated detail",
                    exec_module
                )
                result = db.execute_block(delete_old_detail)
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    result,
                    exec_module
                )
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    "Deleting old generated report",
                    exec_module
                )
                result = db.execute_block(delete_old_report)
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    result,
                    exec_module
                )
                result = db.execute_block(delete_old_report_legacy)
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    result,
                    exec_module
                )
            
            result = db.drop_table(f"{temporal_schema}.{temp_transaction_table}")
            result = db.drop_table(f"{temporal_schema}.{temp_report_table}")
            result = db.drop_table(f"{temporal_schema}.{temp_report_table_legacy}")            

            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Temporal tables droped sucessfully",
                exec_module
            )

            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Report of {month_name} has been saved sucesfully",
                exec_module
            )

        except Exception as e:

            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "CRITICAL",
                f"UNHANDLED EXCEPTION: {str(log.logs().print_except())}",
                exec_module
            )

    def read_table(self, client: str, file: str, date: str):
        """Reads a csv on the scheme fee's bucket in the clients 'IN' directory with feeded date data
        
        Args:
            client (str): must exists in database.
            date (str): must be in format YYYYMM.
            file (str): must be route IN to file and exists in bucket

            
        """
        try:
            
            tran = None
            schem = "operational"
            temp_schem = "temporal"
            module = "OPERATIONAL"
            exec_module = "SCHEME FEE"
            s3_route = "IN/"+file
            step =  ""

            log_name = log.logs().new_log(
                module, "", client, "GENERATION OF SCHEME FEE REPORT", "SCHEME_FEE",exec_module
            )

            date = str(date)
            year = date[0:4]
            month = date[4:6]
            date_month = calendar.monthrange(int(year),int(month))

            first_day = datetime.strptime(f"{year}-{month}-01 00:00:00","%Y-%m-%d %H:%M:%S")
            last_day = datetime.strptime(f"{year}-{month}-{date_month[1]} 23:59:59","%Y-%m-%d %H:%M:%S")

            month_of_report = datetime.strptime(month, "%m")
            month_name = date
            
            step = "READING SCHEME FEE UPDATE TEMPLATE"

            log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    f"Reading file from S3 route: IN/{client}/{file}",
                    exec_module
                )

            date_of_extract = datetime.now()
            formated_date = (
                str(date_of_extract.year)
                + str(date_of_extract.month)
                + str(date_of_extract.day)
                + "_"
                + str(int(datetime.timestamp(date_of_extract)))
            )


            s3 = connection.connect_to_s3()
            s3_bucket =  os.getenv("SCHEME_FEE_BUCKET")
            s3_route = f"IN/{client}/{file}"
            filename = pathlib.Path(file).name
            temp_route_folder = f"FILES/SCHEME_FEE/IN/{client}/{date}/{formated_date}"
            temp_file_name = f"{temp_route_folder}/{filename}"
            temp_transaction_table = f"temp_scheme_fee_transaction_{client.lower()}_{date}"
            temp_report_table = f"temp_scheme_fee_report_{client.lower()}_{date}"
            pathlib.Path(temp_route_folder).mkdir(parents=True, exist_ok=True)

            s3.get_object(s3_bucket,s3_route,temp_file_name)
            valid_NaNs = ['-1.#IND', '1.#QNAN', '1.#IND', '-1.#QNAN', '#N/A N/A', '#N/A', 'N/A', 'n/a', '', '#NA', 'NULL', 'null', 'NaN', '-NaN', 'nan', '-nan', '']

            df = pd.read_csv(temp_file_name,sep=",",encoding="Latin-1",keep_default_na=False,na_values=valid_NaNs, dtype={"mct_cd":str,
                "prg_id":str,
                "txn_rvsl_flg_id":str,
                "txn_crncy_lcl_flg_id":str,
                "txn_crd_prs_flg_id":str,
                })

            if len(df.index) == 0:
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    f"No data has been found in the file. closing process",
                    exec_module
                )
                return 

            log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    f"procesing file data of month: {month_name}",
                    exec_module
                )


            df["unt_sfc"] = round(df["txn_sfc"] / df["txn_cnt"],6)
            df["unt_est_sch_fee_amt"] = round(df["est_sch_fee_amt"] / df["txn_cnt"],6)
            
            querys = getquery.getquery(client)
            db = connection.connect_to_postgreSQL()

            report_sumary = db.select("operational.mh_scheme_fee_sumary",f"where report_month = '{month_name}' and  report_client_code = '{client}'")
            param_execution_id = report_sumary[0]["app_execution_id"]
            param_issuer_acquirer = ['in','out']

            update_from_temp = querys.get_update_from_temp(temp_schem,temp_report_table,month_name,param_execution_id)    

            if len(report_sumary) == 0 :
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "ERROR",
                    f"Report is not generated yet or not exists anymore.",
                    exec_module
                )

                exit() 

            if len(df.index) != report_sumary[0]["number_of_groups"]:
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "WARNING",
                    f"The quantity of records in file is not the same as the one registered in the report.{len(df.index)} - {report_sumary[0]['number_of_groups']} | closing execution",
                    exec_module
                )
                exit()
            step = "UPDATING DATA"

            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Droping temporal table if exists",
                exec_module
            )
            result = db.drop_table(f"{temp_schem}.{temp_report_table}")
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result,
                exec_module
            )
            current_time = datetime.now().time() 
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Inserting data into temporal table {temp_schem}.{temp_report_table} at {current_time}",
                exec_module
            )
            result = db.insert_from_dataframe(temp_report_table,dataframe=df,schema=temp_schem,if_exists="replace",dtype={"tkt_siz_id":sqlalchemy.String})
            current_time = datetime.now().time() 
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Data inserted sucesfully. Rows inserted {len(df)} at {current_time}",
                exec_module
            )
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Updating data in report table",
                exec_module
            )
            result = db.execute_block(update_from_temp,True)
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result[0],
                exec_module
            )
            current_time = datetime.now().time()
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Updates in report table finished at {current_time}",
                exec_module
            )
            if int(result[1]) != report_sumary[0]["number_of_groups"] :
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "CRITICAL",
                    f"Updated groups quantity ({result[1]} rows) is not the same as the report sumary ({report_sumary[0]['number_of_groups']} rows) . Aborting process.",
                    exec_module
                )
                exit()

            current_time = datetime.now().time()
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Updating transactions in transactions table at {current_time}",
                exec_module
            )

            total_rows_updated = 0
            begin_date = first_day
            form_date = last_day
            array_dates=[]

            while begin_date <= form_date:
                array_dates.append(begin_date.strftime("%Y%m%d"))
                begin_date += timedelta(days=1)

            for iss_acq_ind in param_issuer_acquirer:
                if iss_acq_ind is not None:

                    for partition_date in array_dates:
                        if partition_date is not None:

                            update_detail = querys.get_update_detail(iss_acq_ind,partition_date,month_name,param_execution_id)
                            result = db.execute_block(update_detail,True)

                            total_rows_updated = total_rows_updated + result[1]

            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                result[0],
                exec_module
            )
            current_time = datetime.now().time()
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Updates in transactions table finished at {current_time}",
                exec_module
            )
            if int(total_rows_updated) != report_sumary[0]["number_of_inserted_rows"] :
                log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "CRITICAL",
                    f"Updated rows quantity ({total_rows_updated} rows) is not the same as the report sumary ({report_sumary[0]['number_of_inserted_rows']} rows) . Aborting process.",
                    exec_module
                )
                exit()

            update_sumary = querys.get_update_sumary(total_rows_updated,month_name)
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Saving results in database",
                exec_module
            )

            result = db.execute_block(update_sumary)

            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                "Report sumary updated sucesfully",
                exec_module
            )

            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Closing conection to database",
                exec_module
            )

            log.logs().exist_file(
                    module,
                    client,
                    "VISA AND MASTERCARD",
                    log_name,
                    step,
                    "INFO",
                    f"Tables updated sucessfully",
                    exec_module
                )
    
        except Exception as e:
            log.logs().exist_file(
                "OPERATIONAL",
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "CRITICAL",
                f"UNHANDLED EXCEPTION: {str(log.logs().print_except())}",
                exec_module
            )
    
    def input_validate(self,client:str,module:str,log_name:str,step:str,month_name:str,exec_module:str)-> bool:
        """Ask for input returns true or false
        
        Args:
            client (str): clients code
            module (str): type of module.
            log_name (str): log name.
            step (str): step on execution.
            month_name (str): month of report
            exec_module (str): execution module.

        Returns:
            bool: True if is not a valid input, False if is a valid input.
        
        """
        yes_array = ['yes','y','si','ya','ok']
        no_array = ['no','n','nah','nope']
        asking = input("A report has been already generated. If you want to continue, this will delete all data from the previous report. Type 'Yes' to continue or 'No' to cancel: ")
        asking = asking.lower()
        if asking in yes_array:
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "WARNING",
                f"Previous data is going to be deleted . Month: {month_name}",
                exec_module
            )
            return False

        elif asking in no_array:
            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Canceled by user",
                exec_module
            )
            exit()
        
        else:
            print("'"+asking+"' is not a valid input")
            return True