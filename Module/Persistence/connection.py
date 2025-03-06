from datetime import date, datetime, timedelta                                              
import pandas as pd
import os
import sys
from typing import Any, List
import boto3
import pgdb
import json
import botocore
import Logs.logs
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta


class connect_to_s3:
    """Connection methods to s3 repository."""

    def __init__(self) -> None:
        load_dotenv()
        self.log_name = None
        self.typeLog = None
        self.client = None
        self.exec_module = None

    def loading_credentials(self, bucket: str) -> List:
        """Loading credentials from .env config file.
        
        Args:
            bucket (str): bucket to be accesed.
    
        Returns:
            List: list of credentials.

        """
        try:
            buckets = self.get_buckets()
            if bucket in buckets:
                aws_region = os.getenv("AWS_DEFAULT_REGION_1", "us-west-2")
                aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID_1", "")
                aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY_1", "")
            else:
                print("CRITICAL ERROR, BUCKET IS NOT FOUND. Terminating script")
                sys.exit()
            return [
                aws_region,
                aws_access_key_id,
                aws_secret_key,
            ]
        except Exception as error:
            print("CRITICAL ERROR, BUCKET IS NOT FOUND. Terminating script")
            sys.exit()

    def __connect_s3(self, bucket: str) -> object:
        """Connection to s3 buckets 
        Args:
            bucket: bucket to be accessed.

        Returns 
            object: boto3 object.
        """
        try:
            credentials = self.loading_credentials(bucket)
            s3 = boto3.resource(
                service_name="s3",
                region_name=credentials[0],
                aws_access_key_id=credentials[1],
                aws_secret_access_key=credentials[2],
            )
            return s3
        except Exception as error:
            print("CRITICAL ERROR :" + str(error) + " ; Terminating script")
            sys.exit()
        except botocore.errorfactory.NoSuchBucket as error:
            print("CRITICAL ERROR, BUCKET IS NOT FOUND. Terminating script")
            sys.exit()

    def get_buckets(self) -> list:
        """get list of buckets from enviroment.
        
        Returns:
            buckets (list): list of buckets.
        
        """
        buckets = json.loads(os.getenv("BUCKETS"))
        return buckets

    def upload_object(self, bucket: str, path_to_file: str, path_to_save: str) -> bool:
        """Upload object to the specified bucket, returns True/False.
        
        Args:
            bucket (str): destiny bucket.
            path_to_file (str): path to file to be uploaded.
            path_to_save (str): destiny path in bucket.
        
        Returns:
            bool: True if is successfully uploaded, False if not.
        
        """
        try:
            s3 = connect_to_s3().__connect_s3(bucket)
            s3.Bucket(bucket).upload_file(Filename=path_to_file, Key=path_to_save)
            return True
        except boto3.exceptions.S3UploadFailedError as error:
            if self.typeLog is not None:
                Logs.logs.logs().exist_file(self.typeLog,self.client,"VISA AND MASTERCARD",self.log_name,"UPLOADING FILE TO S3","CRITICAL",f"Exception {error} | terminating script ",self.exec_module,upload=False)
            else:
                print(f"Exception {error} | terminating script ")
            return False,error

    def get_object(
        self, bucket: str, path_to_file: str, path_to_save: str, download: bool = True
    ) -> bool | Any:
        """Download specified object from bucket
        
        Args:
            bucket (str): destiny bucket.
            path_to_file (str): path to file in bucket.
            path_to_save (str): destiny path in local path.  
            download (bool): 

        Returns:
            Any: True/False or Response object.
        
        """
        try:
            s3 = connect_to_s3().__connect_s3(bucket)
            if download:
                """Download object"""
                result = s3.Bucket(bucket).download_file(
                    Key=path_to_file, Filename=path_to_save
                )

                return True
            else:
                """Gets response dict as return"""
                response = s3.Bucket(bucket).Object(path_to_file).get()
                return response
        except botocore.exceptions.ClientError as Error:
            print(Error)
            return False

    def delete_object(self, bucket: str, path_to_file: str) -> bool:
        """Deletes specified object at bucket
        
        Args:
            bucket (str): destiny bucket.
            path_to_file (str): path to file in bucket

        Returns:
            bool: True if success or False if fails.
        
        """
        s3 = connect_to_s3().__connect_s3(bucket)
        result = s3.Object(bucket, path_to_file).delete()
        return True if (result["ResponseMetadata"]["HTTPStatusCode"] == 204) else False

    def list_content(self, bucket: str, filter: str = None) -> list:
        """lists the contents of the specified bucket
        
        Args:
            bucket (str): bucket to be searched.
            filter (str): filter or folder in bucket.
        
        Returns:
            list_of_objects (list): list of contents of specified bucket

        """
        s3 = connect_to_s3().__connect_s3(bucket)
        filter = "" if (filter == None) else filter
        list_of_objects = []
        for obj in s3.Bucket(bucket).objects.filter(Delimiter="\\", Prefix=filter):
            list_of_objects.append(obj.key)
        return list_of_objects


class connect_to_postgreSQL:
    """Connection to postgreSQL database defined in .env"""

    def __init__(self, bool_query:bool = False) -> None:
        load_dotenv()
        self.bool_query = bool_query
        self.query = ''

    def __loading_credentials(self) -> list:
        """Loading credentials from .env config file
        
        Returns:
            list: list with credentials.
        """
        database = os.getenv("POSTGRE_DATABASE", "postgres")
        host = os.getenv(
            "POSTGRE_HOST",
            "db-dev-intelica-instance-1.cf3zxr6zcsiz.us-east-1.rds.amazonaws.com",
        )
        port = os.getenv("POSTGRE_PORT", "5432")
        user = os.getenv("POSTGRE_USER", "postgres_dev")
        password = os.getenv("POSTGRE_PASSWORD", "7s1Jy5ewQAu0NfDO9iwV")
        return [database, host, port, user, password]

    def conecction(self) -> object:
        """Returns a connection class already initializated for postgreSQL database
            Example of use:
                connecting to database
                x = connect_to_postgreSQL().conecction()
                cur = x.cursor()
                Exec function
                cur.execute('SELECT version()')
                Fetching result
                db_version = cur.fetchone()
                shows version
                print(db_version)
                then we close cursor
                cur.close()

            Returns:
                object: connection object.
        """
        try:
            credentials = self.__loading_credentials()
            connection = pgdb.Connection(
                database=credentials[0],
                host=credentials[1],
                user=credentials[3],
                password=credentials[4],
                **{"port": credentials[2]},
            )
            return connection
        except Exception as e:
            print(e)
            return [(False, e)]

    def prepare_engine(self) -> object:
        """Creates an engine with sqlAlchemy using the credentials
        
        Returns:
            object: Engine object.
        """
        credentials = self.__loading_credentials()
        connection_string = f"postgresql://{credentials[3]}:{credentials[4]}@{credentials[1]}/{credentials[0]}"
        return create_engine(connection_string)

    def execute_block(self, query: str, return_row_count: bool = False) -> Any:
        """Executes a query
        
        Args:
            query (str): query string
            return_row_count (bool): return rows affected as message or as number.

        Returns:
            Any:  result_message if return_row_count = False or a list with the message and rows afectected.
        """
        try:
            conn = self.conecction()
            cur = conn.cursor()
            cur.execute(query)
            result_message = f"Query successfully executed, {cur.rowcount} row(s) affected"
            conn.commit()
            cur.close()
            conn.close()
            if return_row_count:
                return [result_message,cur.rowcount]
            else:
                return result_message
        except Exception as e:
            raise
    def insert(self, query: str, records) -> Any:
        """Executes insert query, returns message

        Args:
            query (str): query string
            records (Any): a list of records to be insrted.

        Returns:
            result_message (str): Message.
        
        """
        try:
            conn = self.conecction()
            cur = conn.cursor()
            sql_insert_query = query
            result = cur.executemany(sql_insert_query, records)
            result_message = "records inserted/updated successfully into table"
            conn.commit()
            cur.close()
            conn.close()
            return result_message
        except Exception as e:
            raise

    def insert_log(self, records_list) -> str:
        """Insert into Table control.t_log, returns message
        
        Args:
            records_list (Any): list of records.

        Returns:
            result_message (str): result message.
        """
        sql_insert_query = """ insert into CONTROL.T_LOG (customer,brand,process_name,process_message,description_status,start_date,end_date,file_name) values (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,NULL,%s)"""
        result_message = self.insert(sql_insert_query, records_list)
        return result_message

    def insert_control_file(self, records_list) -> Any:
        """Insert into Table control.t_control_file , returns message
        
        Args:
            records_list (Any): list of records.

        Returns:
            result_message (str): result message.
        
        """
        sql_insert_query = """ INSERT INTO CONTROL.T_CONTROL_FILE (CODE, BRAND, CUSTOMER, log_file_name, process_file_name,records_number,process_date,file_date,control_message,description_status,file_type,parent_zip,execution_id) VALUES(%s,%s,%s,%s,%s,0,%s,%s,%s,%s,%s,%s,%s)"""
        result_message = self.insert(sql_insert_query, records_list)
        return result_message

    def truncate_table(self, table: str) -> Any:
        """truncate table, returns message
        
        Args:
            table (str): table name.

        Returns:
            result_message (str): result message.
        
        """
        records = [(table)]
        query = "truncate table %s" % table
        result_message = self.insert(query, records)
        result_message = 'truncated : ' + table
        return result_message

    def drop_table(self, table_name: str) ->Any:
        """Drops table, returns message
        
        Args:
            table_name (str): table name.

        Returns:
            result_message (str): result message.
        
        """
        records = [(table_name)]
        query = "drop table if exists %s" % table_name
        if self.bool_query:
            self.query+=query+';'
            result_message = f"{table_name} is going to be droped"
        else:
            result_message = self.insert(query, records)
            result_message = 'droped : ' + table_name

        return result_message

    def get_structure_table_from_db(self, table_scheme: str, table_name: str, order_by: str = 'order by ordinal_position') -> list:
        """Gets table structure from of given table, returns a list with said structure.
        
        Args:
            table_scheme (str): name of schem of table.
            table_name (str): name of table.
            order_by (str): query for order by.

        Returns:

            list: list of rows selected.
        
        """
        query_where = f"where table_schema= '{table_scheme}' and table_name = '{table_name}' {order_by}"
        query_columns = 'table_schema,table_name,column_name,data_type,ordinal_position,character_maximum_length as length,numeric_precision,numeric_scale'
        query_table = 'information_schema.columns'
        return self.select(query_table, query_where.lower(), query_columns.lower())

    def validate_structure(self,source_table : str,target_table : str, column_name : str = 'column_name') -> bool:
        """Validates if a column exists, returns a True/False
        
        Args:
            source_table (str): source table and schema.
            target_table (str): target table and schema.
            column_name (str): target column.

        Returns:
            bool_result (bool): True if exists, False if not.
        
        """
        list_source_table = source_table.split('.')
        list_target_table = target_table.split('.')
        table = f"(select column_name,character_maximum_length from information_schema.columns where table_name = '{list_source_table[1]}' and table_schema = '{list_source_table[0]}' and data_type='character varying') a full join(select {column_name}, length from {target_table} where current_date between start_date and coalesce(end_date,current_date)) b on (a.column_name = b.{column_name} and a.character_maximum_length = b.length)"
        cols = f'case when count(a.column_name) - 5 = count(b.{column_name}) then 1 else 0 end flag_structure'
        bool_result = False
        if self.table_exists(target_table):
            result = self.select(table,None,cols)
            if  result[0]['flag_structure'] == 1:
                bool_result = True
        return bool_result
    
    def table_count(self, table_scheme: str, table_name: str, where_query: str = None)->str:
        """Counts rows in table, returns quantity of rows
        
        Args:
            table_scheme (str): target schema.
            table_name (str): target table name.
            where_query (str): query conditions.

        Returns:
            str : result of count query.
        """
        table = f'{table_scheme}.{table_name}'
        if where_query == None:
            where_query=''
        result = self.select(table,where_query,'count(1) count')
        return str(result[0]['count'])

    
    def table_exists(self, table_name : str) -> bool:
        """Verify if table exists.
        
        Args:
            table_name (str): target table name and schema.

        Returns:
            bool : True if exists, False if not.
        """
        table = f"(SELECT to_regclass('{table_name}') table_exists) dummy"
        result = self.select(table,cols='table_exists')
        if result[0]['table_exists'] == None:
            return False
        else:
            return True
    def add_column(self, list_structure: list, table_scheme: str, table_name: str) -> str:
        """add a column to specified table.
        
        Args:
            list_structure (list): list with columns data to be inserted.
            table_scheme (str): scheme of table.
            table_name (str): table name.

        Returns:
            result_message : result message.
        """
        records = [(table_name)]
        add_counts = 0
        for value in list_structure:
            if value["column_type"] == 'varchar':
                query = f'alter table {table_scheme}.{table_name} add {value["column_name"]} {value["column_type"]}({value["length"]})'
            if value["column_type"] in ['date','integer','bigint','text','numeric','timestamp']:
                query = f'alter table {table_scheme}.{table_name} add {value["column_name"]} {value["column_type"]}'
            else:
                query = f'alter table {table_scheme}.{table_name} add {value["column_name"]} text'
            result_message = self.insert(query, records)
            add_counts+=1
        result_message = f'{add_counts} columns added to table : {table_scheme}.{table_name}'
        return result_message

    def insert_from_table(self, source_table_scheme : str, source_table_name: str, target_table_scheme: str, target_table_name: str)-> str:
        """ inserts from one table to another.

        Args:
            source_table_scheme (str): source table scheme.
            source_table_name (str): source table name.
            target_table_scheme (str): target table scheme.
            target_table_name (str): target table name.

        Returns:
            result_message (str): result message.
        """
        records = [(target_table_name)]
        df_source = pd.DataFrame(self.get_structure_table_from_db(source_table_scheme,source_table_name))
        if df_source.empty:
            return '0 records'
        str_columns = '"' + '","'.join(map(str,list(df_source['column_name'].values))) + '"'
        query = f'insert into {target_table_scheme}.{target_table_name} ({str_columns}) select {str_columns} from {source_table_scheme}.{source_table_name}'
        result_message = self.insert(query,records) + ' ' + target_table_scheme + '.' + target_table_name
        return result_message

    def update_from_table(self, source_table_scheme : str, source_table_name: str, target_table_scheme: str, target_table_name: str, key_columns: list)-> str:
        """ updates from one table to another, both tables must have the same columns names.

        Args:
            source_table_scheme (str): source table scheme.
            source_table_name (str): source table name.
            target_table_scheme (str): target table scheme.
            target_table_name (str): target table name.
            key_columns (list): list of columns to be updated.
        Returns:
            result_message (str): result message.
        """
        records = [(target_table_name)]
        df_source = pd.DataFrame(self.get_structure_table_from_db(source_table_scheme,source_table_name))
        df_source = df_source[~df_source['column_name'].isin(key_columns)]
        query_where = ''
        query_set = ''
        for value in key_columns:
            query_where+=f't.{value} = s.{value} and '
        query_where = query_where[:-4]
        for value in df_source['column_name'].values:
            query_set+=f'{value} = s.{value},'
        query_set = query_set[:-1]
        query_update = f"""
        update {target_table_scheme}.{target_table_name} t
        set {query_set}
        from {source_table_scheme}.{source_table_name} s
        where {query_where}
        """
        result_message = self.insert(query_update,records) + ' ' + target_table_scheme + '.' + target_table_name
        return result_message
    
    def create_table_from_select(self, select_query : str, table: str)-> str:
        """ creates a table from another as base.

        Args:
            select_query (str): base select query of origin table.
            table (str): new table name and schema.

        Returns:
            result_message (str): result message.
        """
        records = [(table)]
        query_create = f'create table {table} as ' + select_query
        result_message = self.insert(query_create,records)
        result_message = f'table created : {table}'
        return result_message

    def create_table_index(self, table_scheme: str, table_name: str, index_name: str, table_column: str)-> str:
        """creates an index column in specified table.
            
        Args:
            table_scheme (str): target table scheme.
            table_name (str): target table name.
            index_name (str): new index name.
            table_column (str): target table column.

        Returns:
            result_message (str): result message.
        
        """
        table = f'{table_scheme}.{table_name}'
        records = [(table)]
        query_index = f'create index if not exists {index_name} on {table}({table_column})'
        result_message = self.insert(query_index,records)
        result_message = f'index created : {table_scheme}.{index_name}'
        return result_message
    
    def drop_table_index(self,index_scheme: str, index_name: str)-> str:
        """drops specified table index
            
        Args:
            index_scheme (str): target index scheme.
            index_name (str): target index name.

        Returns:
            result_message (str): result message.
        
        """

        index = f'{index_scheme}.{index_name}'
        records = [(index)]
        query = f"drop index {index}"
        result_message = self.insert(query, records)
        result_message = 'droped : ' + index
        return result_message
    
    def create_table_partition_list(self, table_scheme: str, table_name: str, partition_scheme: str, partition_name: str, partition_value: str, column_subpartition: str = None, subpartition_type: str = None)->str:
        """ Creates a new table partition.
            
        Args:
            table_scheme (str): target table scheme.
            table_name (str): target table name.
            partition_scheme (str): new partition scheme.
            partition_name (str): new partition name.
            partition_value (str): new partition value.
            column_subpartition (str): column for subpartition.
            subpartition_type (str): subpartition type.

        Returns:
            result_message (str): result message.
        
        """
        records = [(table_name)]
        query_subpartition = ''
        if column_subpartition != None and subpartition_type!= None:
            query_subpartition += f' partition by {subpartition_type}({column_subpartition})'
        query_create = f"create table if not exists {partition_scheme}.{partition_name} partition of {table_scheme}.{table_name} for values in ({partition_value})"
        query_create = query_create + query_subpartition
        result_message = self.insert(query_create, records)
        result = self.create_table_partition_default(table_scheme, table_name, partition_scheme, table_name + '_others')
        result_message = f'partitions added to table : {partition_scheme}.{partition_name}'
        return result_message
    def create_table_partition_range(self, table_scheme: str, table_name: str, partition_scheme: str, partition_name: str, partition_start_value: str, partition_end_value: str = None, partition_range_type: str = 'daily', column_subpartition: str = None, subpartition_type: str = None)->str:
        """ Creates a new table partition range.
            
        Args:
            table_scheme (str): target table scheme.
            table_name (str): target table name.
            partition_scheme (str): new partition scheme.
            partition_name (str): new partition name.
            partition_start_value (str): new partition start value.
            partition_end_value (str): new partition end value.
            partition_range_type (str): new partition range type.
            column_subpartition (str): column for subpartition.
            subpartition_type (str): subpartition type.

        Returns:
            result_message (str): result message.
        
        """
        
        records = [(table_name)]
        query_subpartition = ''
        counts = 0
        if column_subpartition != None and subpartition_type!= None:
            query_subpartition += f' partition by {subpartition_type}({column_subpartition})'
        if partition_end_value == None:
            partition_end_value = partition_start_value
        if partition_range_type == 'daily':
            start_date = datetime.strptime(partition_start_value,'%Y%m%d')
            end_date = datetime.strptime(partition_end_value,'%Y%m%d')
            for value in pd.date_range(start_date,end_date):
                    counts+=1
            query_block = f"""
            do $$
            declare
            r text;
            BEGIN
                for r in 
                    select to_date('{partition_start_value}','yyyymmdd') + s.name::integer from generate_series(0,to_date('{partition_end_value}','yyyymmdd')-to_date('{partition_start_value}','yyyymmdd')) s
                loop
                    execute 'create table if not exists {partition_scheme}.{partition_name}_'||to_char(to_date(r,'yyyy-mm-dd'),'yyyymmdd')||' partition of {table_scheme}.{partition_name} for values from ('''||r||''') to ('''||to_date(r,'yyyy-mm-dd')+1||''') {query_subpartition}';
                end loop;
            END $$;
            """
            result_message = self.execute_block(query_block)
        if partition_range_type == 'monthly':
            start_date = datetime.strptime(partition_start_value[0:6]+'01','%Y%m%d')
            end_date = start_date + relativedelta(months=+1)
            query_create = f"create table if not exists {partition_scheme}.{partition_name}_{start_date.strftime('%Y%m')} partition of {table_scheme}.{table_name} for values from ('{start_date.strftime('%Y-%m-%d')}') to ('{end_date.strftime('%Y-%m-%d')}')"
            query_create = query_create + query_subpartition
            result_message = self.insert(query_create, records)

        result = self.create_table_partition_default(table_scheme, table_name, partition_scheme, partition_name + '_others')
        result_message = f'{counts} partitions added to table : {table_scheme}.{table_name}'
        return result_message

    def query_block(self)->str:
        """ executes a query block
            
        Args:

        Returns:
            result_message (str): result message.
        
        """
        query_block = f"""
        do $$
        declare
        r text;
        BEGIN
                {self.query}
        END $$;
        """
        result_message = self.execute_block(query_block)
        return result_message
    
    def create_table_partition_default(self, table_scheme: str, table_name: str, partition_scheme: str, partition_name: str, column_subpartition: str = None, subpartition_type: str = None)->str:
        """ Creates a new table default partition.
            
        Args:
            table_scheme (str): target table scheme.
            table_name (str): target table name.
            partition_scheme (str): new partition scheme.
            partition_name (str): new partition name.
            column_subpartition (str): column for subpartition.
            subpartition_type (str): subpartition type.

        Returns:
            result_message (str): result message.
        
        """
        records = [(table_name)]
        query_subpartition = ''
        if column_subpartition != None and subpartition_type!= None:
            query_subpartition += f' partition by {subpartition_type}({column_subpartition})'
        query_create = f"create table if not exists {partition_scheme}.{partition_name} partition of {table_scheme}.{table_name} default"
        query_create = query_create + query_subpartition
        result_message = self.insert(query_create, records)
        return result_message

    def create_table(self, list_structure : list = None, table_name : str = None, column_type : bool = False, column_partition : str = None, partition_type : str = None, simple_structure: list = None)-> str:
        """ Creates a new table.
            
        Args:
            list_structure (list): list with structure for table
            table_name (str): target table scheme and name.
            column_type (bool): indicator for specified column type.
            column_partition (str): column for partition.
            column_subpartition (str): column for subpartition.
            partition_type (str): partition type.
            simple_structure (list): simple structure list.

        Returns:
            result_message (str): result message.
        
        """
        records = [(table_name)]
        result = self.table_exists(table_name)
        query_partition = ''
        list_columns = []
        if result:
            result = self.drop_table(table_name)
        if column_partition != None and partition_type != None:
            query_partition+= f'partition by {partition_type}({column_partition})'
        if simple_structure != None:
            list_columns.extend(simple_structure)
        else:
            list_columns.extend(list_structure)
        query = 'create table '+table_name+' ('
        for row in list_columns:
            
            if not column_type:
                if simple_structure != None:
                    query += '"'+row.lower()+'"' + ' text,'
                else:
                    query += '"'+row['column_name'].lower()+'"' + ' varchar('+str(int(row['length']))+'),'
                continue
            if column_type:
                if row['column_type']=='varchar':
                    query += '"'+row['column_name'].lower()+'"' + ' varchar('+str(int(row['length']))+'),'
                    continue
                if row['column_type'] in ['date','integer','bigint','text','numeric','timestamp']:
                    query += f'"{row["column_name"].lower()}" {row["column_type"]},'
                    continue

        query = query + ' app_creation_user text DEFAULT USER,app_creation_date timestamp DEFAULT CURRENT_TIMESTAMP)' + query_partition
        result_message = self.insert(query, records)
        return result_message

    def insert_from_csv(
        self,
        table: str,
        option_text: str,
        bucket: str,
        file_path: str,
        columns_table: str = None,
        truncate_table: bool = True,
    ) -> Any:
        """ insert into table from CSV format.
            
        Args:
            table (str): target table scheme and name.
            option_text (str): options text for query.
            bucket (str): source bucket.
            file_path (str): file path in bucket.
            columns_table (str): columns to extract.
            truncate_table (bool): truncate table before insertion .

        Returns:
            result_message (str): result message.
        
        """
        list_credentials = connect_to_s3().loading_credentials(bucket=bucket)
        columns_table = "" if columns_table == None else columns_table
        records = [(table, columns_table, option_text, bucket, file_path, list_credentials[0],list_credentials[1],list_credentials[2])]
        query = """SELECT aws_s3.table_import_from_s3(%s, %s, %s, aws_commons.create_s3_uri(%s, %s, %s),    aws_commons.create_aws_credentials(%s, %s, ''))"""
        if truncate_table:
            result_truncate = self.truncate_table(table)
        result_insert = self.insert(query, records)
                                                                
        return result_insert

    def select(
        self, table: str, conditions: str = None, cols: str | list = None
    ) -> list:
        """Simple select from any table with conditions, returns List

        Args:
            table (str): target table scheme and name.
            conditions (str): query conditions.
            cols (str): selected columns.

        Returns:
            rows (list): selected rows as list.
        
        """
        try:
            query = "SELECT "

            if isinstance(cols, str):
                columns = cols
            elif isinstance(cols, list):
                columns = ",".join(str(x) for x in cols)
            else:
                columns = "*"

            if conditions == None:
                conditions = ""

            query += columns + " FROM " + table + " " + conditions
            con = self.conecction()
            cursor = con.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            result = cursor.fetchall()
            rows = []
            for row in result:
                row_dict = dict(zip(columns, row))
                rows.append(row_dict)
            cursor.close()
            return rows
        except Exception as e:
            print(str(e))
            return [False, {"Message": str(e)}]

    def insert_from_dataframe(self,table:str,schema:str,dataframe:pd.DataFrame,engine=None,index: bool = False,if_exists:str ="fail",dtype:dict ={})-> int:
        """Inserts data from dataframe using SQLAlchemy engine

        Args:
            table (str): target table and name.
            schema (str): target schema.
            dataframe (DataFrame): data from dataframe.
            engine (object): engine conecction object.
            index (bool): index column in dataframe.
            if_exists: option for dataframe in case of table already exists.
            dtype: type of columns dictionary.

        Result:
            inserted_rows (int): inserted rows number.

        """
        pe = self.prepare_engine()
        engine = pe.connect()
        with engine as con:
            
            inserted_rows = dataframe.to_sql(
                table,
                schema =schema,
                con=con,
                if_exists=if_exists,
                index=index,
                dtype=dtype,
            )

        return inserted_rows
    
    def insert_from_df(self, df : pd.DataFrame, table_schema:str,table_name:str,if_exists:str ='replace',index:bool=False,dtype:dict = None)-> str:
        """Inserts data from dataframe using SQLAlchemy engine

        Args:
            table (str): target table and name.
            schema (str): target schema.
            dataframe (DataFrame): data from dataframe.
            engine (object): engine conecction object.
            index (bool): index column in dataframe.
            if_exists: option for dataframe in case of table already exists.
            dtype: type of columns dictionary.

        Result:
            result_message (str): inserted rows number.

        """
        df = df.replace('nan', None).replace('NaT', None)
        pe = self.prepare_engine()
        en = pe.connect()
        result_message=df.to_sql(
            name = table_name,
            schema =table_schema,
            con=en,
            if_exists=if_exists,
            index=index,
            dtype=dtype
        )
        return result_message

    def select_to_df_object(self,query:str)-> pd.DataFrame:
        """inserts into dataframe from select query
        
        Args:
            query (str): select query.

        Returns:
            df (DataFrame): dataframe with data.
        
        """
        pe = self.prepare_engine()
        df = None
        with pe.connect() as con:
            df = pd.read_sql(query,con=con)
        return df

    def update( self, table: str, conditions: str, cols: dict)-> List:
        """Simple update query using dictionary {col:value}, only works with condition
        
        Args:
            table (str): table name.
            conditions (str): conditions for update.
            cols (dict): dictionary of update columns.

        Returns:
            List: list with message and confirmation bool.
        
        """
        try:
            query = "UPDATE " + table + " SET "
            column_data = []
            for key in cols:
                par_value = f"{key}='{cols[key]}'"
                column_data.append(par_value)
            
            if(len(column_data) > 0):
                query+= ",".join(x for x in column_data)
            else:
                return [False,{"Message":"Malformed query"}]
            
            query+=" "+conditions
            conn = self.conecction()
            cursor = conn.cursor()
            cursor.execute(query)
            updated_rows = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            return [True,{"Message":f"updated_rows: {updated_rows}"}]
        except Exception as e:
            print(str(e))
            return [False, {"Message":str(e)}]

