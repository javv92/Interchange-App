import Module.Logs.logs as log
import Module.Persistence.connection as connection
import Module.SchemeFee.getquery as getquery
import Module.Adapter.adapters as adapters
from datetime import datetime
from dotenv import load_dotenv
import calendar


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
            module = "OPERATIONAL"
            exec_module = "SCHEME FEE"

            log_name = log.logs().new_log(
                module, "", client, "GENERATION OF SCHEME FEE REPORT", "SCHEME_FEE", exec_module
            )

            date = str(date)
            year = date[0:4]
            month = date[4:6]
            date_month = calendar.monthrange(int(year), int(month))

            date_of_extract = datetime.now()
            formated_date = (
                str(date_of_extract.year)
                + str(date_of_extract.month)
                + str(date_of_extract.day)
                + "_"
                + str(int(datetime.timestamp(date_of_extract)))
            )

            first_day = datetime.strptime(f"{year}-{month}-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            last_day = datetime.strptime(f"{year}-{month}-{date_month[1]} 23:59:59", "%Y-%m-%d %H:%M:%S")
            month_name = date

            go_main = adapters.get_others(client, log_name)
            begin_date = datetime.strftime(first_day, "%Y%m%d")
            exists_month = connection.connect_to_postgreSQL().select(
                table="operational.mh_scheme_fee_sumary",
                conditions=f"where report_month = '{date}' and report_client_code = '{client}'"
            )
            previous = len(exists_month) > 0
            # go_main.config_additional_table(begin_date,form_date, 'mh_transaction_scheme_fee')
            go_main.config_additional_table(begin_date, begin_date, config_table_name='mh_monthly_scheme_fee')

            temporal_schema = "temporal"
            temp_transaction_table = f"temp_scheme_fee_transaction_{client.lower()}_{date}"
            temp_report_table = f"temp_scheme_fee_report_{client.lower()}_{date}"

            db = connection.connect_to_postgreSQL()

            querys = getquery.getquery(client)

            create_temporal_report = querys.temp_table_scheme_fee_report()

            inserting_data_to_rpt_table = querys.get_insert_into_report_table(
                temporal_schema, temp_report_table,
                'operational', 'mh_transaction_scheme_fee',
                formated_date, month_name,
                first_day, last_day
            )
            updating_temp_report = querys.get_report_extra_columns(temporal_schema, temp_report_table, '', first_day)

            delete_old_report = querys.get_delete_report(month_name, formated_date)

            step = "PREPARING DATA FOR REPORT"

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
            result = db.create_table_from_select(create_temporal_report, f"{temporal_schema}.{temp_report_table}")

            log.logs().exist_file(
                module,
                client,
                "VISA AND MASTERCARD",
                log_name,
                step,
                "INFO",
                f"Create temporal table {temporal_schema}.{temp_transaction_table}",
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

            step = "CLEANING AND FINISHING PROCESS"

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
            result = db.insert_from_table(temporal_schema, temp_report_table, "operational", "mh_monthly_scheme_fee")
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

            # result = db.drop_table(f"{temporal_schema}.{temp_report_table}")
            
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
