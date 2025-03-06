import argparse
import concurrent.futures
from datetime import datetime, date
from dataclasses import dataclass, asdict
import Module.Persistence.connection as con
import Module.Logs.logs as log
from exec_adapter import exec_adapter as ExecAdapter


@dataclass
class Parameters:
    """ DataClass represents the input parameter entity """
    customer_code: str
    brand: str
    start_date: date
    end_date: date

    def start_date_format(self):
        return self.start_date.strftime('%Y-%m-%d')

    def end_date_format(self):
        return self.end_date.strftime('%Y-%m-%d')

    def brand_adapter_format(self):
        if self.brand == 'VI':
            return 'visa'

        if self.brand == 'MC':
            return 'mastercard'

    def brand_log_format(self):
        if self.brand == 'VI':
            return 'VISA'

        if self.brand == 'MC':
            return 'MASTERCARD'


@dataclass
class ReprocessFile:
    """ DataClass represents the file entity to process """
    brand: str
    number_file: int
    hash_file: str
    type_file: str
    header_date: str

    def partition(self):
        return f'{self.type_file.lower()}_{self.header_date}'


class CalculateInterchangePipeline:
    """Execution pipeline"""

    @staticmethod
    def cli():
        parser = argparse.ArgumentParser(add_help=False)

        parser.add_argument(
            "-c",
            "--customer_code",
            help="client code, example: BTRO"
        )
        parser.add_argument(
            "-b",
            "--brand",
            help="Transaction brand VISA / MC"
        )
        parser.add_argument(
            "-sd",
            "--start_date",
            help="start date parameter, example: 20221011"
        )
        parser.add_argument(
            "-ed",
            "--end_date",
            help="end date parameter, example: 20221031"
        )

        return parser.parse_args()

    @staticmethod
    def prepare(cli_args):
        start_date = datetime.strptime(cli_args.start_date,
                                       "%Y%m%d").date()
        end_date = datetime.strptime(cli_args.end_date,
                                     "%Y%m%d").date()

        return Parameters(
            customer_code=cli_args.customer_code,
            brand=cli_args.brand,
            start_date=start_date,
            end_date=end_date
        )

    @staticmethod
    def execute(parameters):
        calculate_interchange = CalculateInterchange(parameters)
        calculate_interchange.execute()


class CalculateInterchange:
    """Calculate Interchange"""

    def __init__(self, parameters: Parameters):
        self.module = 'ADAPTER'
        self.parameters = parameters
        self.customer_code = parameters.customer_code

        self.ps = con.connect_to_postgreSQL()
        self.log_name = self.make_log()

        log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            self.parameters.brand_log_format(),
            self.log_name,
            f"INTERCHANGE OF {self.parameters.brand_log_format()} FILE",
            "INFO",
            f"REPROCESS INTERCHANGE OF {self.parameters.brand_log_format()} "
            f"BETWEEN {self.parameters.start_date_format()} and {self.parameters.end_date_format()}",
            self.module
        )

    def get_reprocess_files(self):

        control_files = self.ps.select(
            """"control".t_control_file""",
            f"""where description_status = 'PROCESSED'
            and customer = '{self.customer_code}'
            and brand = '{self.parameters.brand}'
            and (file_date between '{self.parameters.start_date_format()}' and '{self.parameters.end_date_format()}')
            and file_type in ('IN', 'OUT')
            order by file_type, file_date;
            """,
            """row_number() OVER (ORDER BY file_type, file_date) number_file, code hash_file, file_type type_file, 
            concat(
            left(cast(file_date as varchar(10)),4),
            substring(cast(file_date as varchar(10)),6,2),
            right(cast(file_date as varchar(10)),2)
            ) header_date
            """
        )

        reprocess_files = [ReprocessFile(brand=self.parameters.brand_adapter_format(),
                                         **control_file) for control_file in control_files]
        return reprocess_files

    def delete_visa(self, reprocess_files):
        unique_partitions = {}
        unique_partitions_sms = {}
        querys = []

        for reprocess_file in reprocess_files:
            partition = reprocess_file.partition()
            if partition not in unique_partitions:
                unique_partitions.update({partition: []})
            unique_partitions[partition].append(reprocess_file.hash_file)

            if partition not in unique_partitions_sms:
                unique_partitions_sms.update({partition: []})
            unique_partitions_sms[partition].append(reprocess_file.hash_file)

        for partition, files in unique_partitions.items():
            files_str = ','.join([f"'{app_hash_file}'" for app_hash_file in files])
            query = f"""
            delete from operational.dh_visa_interchange_{self.customer_code.lower()}_{partition}
            where app_hash_file in ({files_str})
            """

            querys.append(query)

        for partition, files in unique_partitions_sms.items():
            files_str = ','.join([f"'{app_hash_file}'" for app_hash_file in files])
            query = f"""
            delete from operational.dh_visa_sms_interchange_{self.customer_code.lower()}_{partition}
            where app_hash_file in ({files_str})
            """

            querys.append(query)

        return querys

    def delete_mastercard(self, reprocess_files):
        unique_partitions = {}
        querys = []
        for reprocess_file in reprocess_files:
            partition = reprocess_file.partition()
            if partition not in unique_partitions:
                unique_partitions.update({partition: []})
            unique_partitions[partition].append(reprocess_file.hash_file)

        for partition, files in unique_partitions.items():
            files_str = ','.join([f"'{app_hash_file}'" for app_hash_file in files])
            query = f"""
            delete from operational.dh_mastercard_interchange_{self.customer_code.lower()}_{partition}
            where app_hash_file in ({files_str})
            """

            querys.append(query)

        return querys

    def delete_by_partition(self, reprocess_files):
        querys = []

        if self.parameters.brand == 'VI':
            querys = self.delete_visa(reprocess_files)

        elif self.parameters.brand == 'MC':
            querys = self.delete_mastercard(reprocess_files)

        self.ps.execute_block(';'.join(querys))

        log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            self.parameters.brand_log_format(),
            self.log_name,
            f"INTERCHANGE OF {self.parameters.brand_log_format()} FILE",
            "INFO",
            f"DELETE INTERCHANGE OF DATA {self.parameters.brand_log_format()} "
            f"BETWEEN {self.parameters.start_date_format()} and {self.parameters.end_date_format()}",
            self.module
        )

    def make_log(self):
        return log.logs().new_log(
            "OPERATIONAL",
            "",
            self.customer_code,
            "Reprocess Calculate Interchange",
            "VISA AND MASTERCARD",
            self.module
        )

    def execute(self):
        reprocess_files = self.get_reprocess_files()
        self.delete_by_partition(reprocess_files)
        list_args = [asdict(reprocess_file) for reprocess_file in reprocess_files]
        exec_adap = ExecAdapter(self.customer_code, self.log_name)
        with concurrent.futures.ProcessPoolExecutor(5) as executor:
            executor.map(exec_adap.execution_interchange_rules, list_args)

        log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            self.parameters.brand_log_format(),
            self.log_name,
            f"INTERCHANGE OF {self.parameters.brand_log_format()} FILE",
            "INFO",
            f"FINISH REPROCESS INTERCHANGE OF {self.parameters.brand_log_format()} "
            f"BETWEEN {self.parameters.start_date_format()} and {self.parameters.end_date_format()}",
            self.module
        )


if __name__ == '__main__':
    """
    Execute Interchange Reprocess.

    Example:
    python exec_interchange.py -c BTRO -b VI -sd 20221201 -ed 20221231
    """
    cli_args = CalculateInterchangePipeline.cli()
    parameters = CalculateInterchangePipeline.prepare(cli_args)
    CalculateInterchangePipeline.execute(parameters)
