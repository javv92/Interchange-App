from logging import exception
from math import nan
import os
from datetime import datetime
import sqlalchemy
from numpy import string_
from urllib3 import get_host
import Module.Persistence.connection as con
import Module.Logs.logs as log
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
import os,sys
import shutil
import pathlib
import psycopg2
import csv
import Module.DataQuality.dq_profilling as dq_p
import Module.DataQuality.dq_cleaning as dq_clean


class get_adapters:
    """Class for adapter's method

    Params:
        customer_code (str): customer code
        log_name (str): name of logs
    
    """
    def __init__(self, customer_code: str, log_name: str):
        load_dotenv()
        self.ps = con.connect_to_postgreSQL()
        self.s3 = con.connect_to_s3()
        self.structured = os.getenv("STRUCTURED_BUCKET")
        self.log = os.getenv("LOG_BUCKET")
        self.customer_code = customer_code
        self.log_name = log_name
        self.module = 'ADAPTER'
        self.path_yml = "Module/DataQuality/config/dq_profilling_tipodato.yml"
        self.schema= "data_review"
        path_to_file = "FILES/ADAPTERS/"
        self.debug = os.getenv("ENV_DEBUG")
        pathlib.Path(path_to_file).mkdir(parents=True, exist_ok=True)
        log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                "Mode debug : "
                + self.debug,
                self.module
            )

    def visa_read_sms(self, df: pd.DataFrame , list_columns : list, column_tcr : str, base_count : int, sc_sms: int=0, log_name: str = None, client:str = None) -> pd.DataFrame:
        """SMS type reading function

        Args:
            df (dataframe) : dataframe with data
            list_columns (list): list of columns
            column_tcr (str): column with tcr data
            base_count (int): counter

        Returns:
            pd.DataFrame: Dataframe with data
        
        """
        list_sms = []
        dict_sms = {}
        count = base_count
        module = 'ADAPTER'
        control = False

        for index, value in enumerate(df[column_tcr].values):
            if value[40-sc_sms:42-sc_sms] == '00':
                if control:
                    list_sms.append(dict_sms)
                else:
                    control = True
                dict_sms = {}
                count += 1
                dict_sms.update({list_columns[0] : count})
                for base in range(1,len(list_columns)):
                    dict_sms.update({list_columns[base] : df[list_columns[base]].values[index]})
                dict_sms.update({value[36-sc_sms:42-sc_sms] : value})
            elif control:
                dict_sms.update({value[36-sc_sms:42-sc_sms] : value})
        list_sms.append(dict_sms)
        return pd.DataFrame(list_sms)

    def visa_read_condition(self, condition: str, delimiter: str, log_name: str = None, client:str = None) -> list:
        """convert to a readable condition list

        Args:
            condition (str): string of conditions
            delimiter (str): conditions delimiter (& or |)

        Returns:
            list_return (list): readable condition list
        """
        module = 'ADAPTER'

        list_condition = []
        list_return = []
        list_condition = condition.split(delimiter)
        for string in list_condition:
            if string[0:1] == 'P':
                value_name = string[0:1]
                substring = string[1:string.find(' ')].split('-')
                string_start_position = int(substring[0])
                string_end_position = int(substring[1])
                if string.find('==')>-1:
                    value_start_position = string.find('==')
                if string.find('!=')>-1:
                    value_start_position = string.find('!=')
                string_value = string[value_start_position+3:].replace("'",'')
                string_tcr = None
                string_operator = string[value_start_position:value_start_position + 2]
            elif string[0:3] == 'TCR':
                value_name = string[0:3]
                list_string = string.split(':')
                string_tcr = list_string[0].replace('TCR ','')
                substring = list_string[1][2:list_string[1].find(' =')].split('-')
                string_value = list_string[1][list_string[1].find('==')+3:].replace("'",'')
                string_start_position = int(substring[0])
                string_end_position = int(substring[1])
                string_operator = '=='
            list_return.append({'value_name' : value_name,'value_start_position' : string_start_position, 'value_end_position' : string_end_position, 'value' : string_value, 'value_tcr' : string_tcr, 'value_operator' : string_operator})
        return list_return

    def visa_apply_condition(self, df : pd.DataFrame, column_apply_condition: str, list_condition : list ,operator : str , skip_character : int, skip_columns : int, log_name: str = None, client:str = None) -> pd.DataFrame:
        """Visa conditions applicator

        Args:
            df (dataframe): dataframe with data
            column_apply_condition (str): column conditions
            list_condition (list): list of conditions
            operator (str): operator for condition
            skip_character (int): characters to skip in reading
            skip_columns (int): columns to skip

        Returns:
            pd.Dataframe: Dataframe with data    
        
        """
        if operator == '&':
            for value in list_condition:
                if value[0:1] == 'P':
                    substring_condition = value[1:value.find(' ')].split('-')
                    condition_start_position = int(substring_condition[0]) + skip_character
                    condition_end_position = int(substring_condition[1]) + skip_character + 1
                    string_condition_value = value[value.find('==')+3:].replace('  ','_').replace("'",'')
                    df = df[df[column_apply_condition].str[condition_start_position:condition_end_position].replace('  ','_') == string_condition_value]
                if value[0:3] == 'TCR':
                    list_string_condition = value.split(':')
                    tcr_condition = list_string_condition[0].replace('TCR ','')
                    if tcr_condition == '0':
                        column_condition = str(skip_columns)
                    substring_condition = list_string_condition[1][2:list_string_condition[1].find(' =')].split('-')
                    condition_start_position = int(substring_condition[0]) + skip_character
                    condition_end_position = int(substring_condition[1]) + skip_character + 1
                    string_condition_value = list_string_condition[1][list_string_condition[1].find('==')+3:].replace('  ','_').replace("'",'')
                    df = df[df[column_condition].str[condition_start_position:condition_end_position].replace('  ','_') == string_condition_value]
            return df
        elif operator =='|':
            df_tmp = pd.DataFrame()
            df_tmp = df_tmp.join(df.head(0),how='right')
            for value in list_condition:
                df_condition_tmp = pd.DataFrame()
                if value[0:1] == 'P':
                    substring_condition = value[1:value.find(' ')].split('-')
                    condition_start_position = int(substring_condition[0]) + skip_character
                    condition_end_position = int(substring_condition[1]) + skip_character + 1
                    string_condition_value = value[value.find('==')+3:].replace('  ','_').replace("'",'')
                    df_condition_tmp = df_condition_tmp.join(df[df[column_apply_condition].str[condition_start_position:condition_end_position].replace('  ','_') == string_condition_value],how='right')
                if value[0:3] == 'TCR':
                    list_string_condition = value.split(':')
                    tcr_condition = list_string_condition[0].replace('TCR ','')
                    if tcr_condition == '0':
                        column_condition = str(skip_columns)
                    substring_condition = list_string_condition[1][2:list_string_condition[1].find(' =')].split('-')
                    condition_start_position = int(substring_condition[0]) + skip_character
                    condition_end_position = int(substring_condition[1]) + skip_character + 1
                    string_condition_value = list_string_condition[1][list_string_condition[1].find('==')+3:].replace('  ','_').replace("'",'')
                    df_condition_tmp = df_condition_tmp.join(df[df[column_condition].str[condition_start_position:condition_end_position].replace('  ','_') == string_condition_value],how='right')
                if not df_condition_tmp.empty:
                    df_tmp = pd.concat([df_tmp,df_condition_tmp],ignore_index=True).drop_duplicates()
            if not df_tmp.empty:
                return df_tmp
            else:
                return df
        
    def visa_upload_adapter(self, filename_parquet: str, file_type: str = 'in',hash_file:str = None, number_file:str = None, string_date:str = None)-> str:
        """Load visa adapter based on type as csv object
        
        Args:
            filename_parquet (str): local parquet file to read
            file_type (str): type of file 
            hash_file (str): hash code of file
            number_file (str): number of file in queue
            string_date (str): date of file

        Returns:
            str: Message

        """
        try:
            module = 'ADAPTER'
            log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA",
                self.log_name,
                "ADAPTER OF VISA FILE",
                "INFO",
                "loading adapter file :" + hash_file,
                self.module
            )

            bucket = self.structured
            table_name_adapter = 'control.t_visa_adapter'
            schema_name= 'operational'
            table_name_stg = 'stg_adapter_visa_transaction'
            table_name_stg_base = schema_name + '.' +table_name_stg
            string_date = datetime.now().strftime('%Y%m%d')
            records_adapter = self.ps.select(table_name_adapter,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)")
            df = pd.read_parquet(filename_parquet,engine='pyarrow')
            filename = 'adapter_visa.csv'
            customer = self.customer_code
            skip_columns = 5
            skip_character = -1
            start_position_tcr = 3
            end_position_tcr = 4
            len_tcr = 2
            list_record = []
            list_record_tmp = []
            dict_record = []
            list_base = ['0','1','2','3','4']
            reference_file_length = [168, 170]
            file_length = len(df.iloc[0][skip_columns])
            len_df = len(df.index)
            if file_length == reference_file_length[1]:
                skip_character += 2
                start_position_tcr += 2
                end_position_tcr += 2
                len_tcr += 2
            for record in records_adapter:
                record_tc = record['type_record']
                if record_tc not in list_record:
                    list_record.append(record_tc)
                    dict_record.append({'tc':record['tc'].replace(' ','') , 'type_record' : record['type_record'], 'condition_type_record' : record['condition_type_record']})
            for value in dict_record:
                list_record_tmp+=value['tc'].split(',')
            for row in dict_record:
                table_name = table_name_stg_base

                if row['type_record']!='transaction':
                    table_name += '_'+row['type_record']
                count_data_tc = 0
                filename_record = f"FILES/ADAPTERS/{customer}_{file_type}_{number_file}_{row['type_record']}_{filename}"
                dfnew = pd.DataFrame()
                list_row = row['tc'].split(',')
                select_df = self.visa_apply_condition(df[df[str(skip_columns)].str.startswith(tuple(list_row), na=False)].reset_index(drop=True),str(skip_columns),str(row['condition_type_record']).split('&'),'&',skip_character,skip_columns)
                dfnew = dfnew.join(select_df[list_base].head(0),how='right')
                if row['type_record'] == 'sms':
                    sc_sms = 0
                    if file_length == reference_file_length[0]:
                        sc_sms = 2 #character jump for files of 168
                    df_sms = self.visa_read_sms(select_df,list_base,str(skip_columns), len_df, sc_sms)
                for record in records_adapter:
                    str_tc = record['tc'].replace(' ','')
                    str_type_record = record['type_record']
                    if str_tc==row['tc'] and str_type_record == row['type_record']:
                        str_tcr = record['tcr'].replace(' ','')
                        string_condition = str(record['additional_condition'])
                        if record['position']>2 or file_length == reference_file_length[0]:
                            start_position_column = record['position'] + skip_character
                            end_position_column = record['position'] + record['length'] + skip_character
                        else:
                            start_position_column = record['position'] - skip_character
                            end_position_column = record['position'] + record['length'] - skip_character
                        name_column = str(record['column_name']).replace(',','').replace('\n','')
                        for column in select_df:
                            if select_df[column].count()>0:
                                str_row_max_len = select_df[column].str.len().max()
                                if str_row_max_len>len_tcr and int(column)>=skip_columns:
                                    df_column = pd.DataFrame()
                                    df_condition = pd.DataFrame()
                                    count_data_column = 0
                                    count_df_condition = 0
                                    str_temp = list(filter(lambda col: col!= None,select_df[column].str[start_position_tcr:end_position_tcr].drop_duplicates().to_list()))
                                    if record['type_record'] == 'header':
                                        df_column = df_column.join(select_df[list_base],how='right')
                                        df_column[name_column] = select_df[str(skip_columns)].str[start_position_column:end_position_column]
                                        dfnew = pd.merge(dfnew,df_column,how='outer')
                                        count_data_tc+=1
                                        break
                                    if str_temp[0] == str_tcr:
                                        select_df_tmp = pd.DataFrame()
                                        list_condition = []
                                        
                                        if string_condition.find('&') != -1:
                                            list_condition = string_condition.split('&')
                                            select_df_tmp = select_df_tmp.join(self.visa_apply_condition(select_df,column,list_condition[1].split('|'),'&',skip_character,skip_columns),how='right')
                                        else:
                                            select_df_tmp = select_df_tmp.join(select_df,how='right')
                                        string_condition_sub = string_condition.split('&')
                                        if (string_condition_sub[0].find('|') != -1 or len(string_condition_sub[0])>0):
                                            list_condition = string_condition_sub[0].split('|')
                                            if row['type_record'] == 'sms':
                                                list_sms = self.visa_read_condition(string_condition_sub[0],'|')
                                                df_condition = df_condition.join(df_sms,how='right')
                                                column = list_sms[0]['value']
                                                if len(list_sms)>1 and column in df_condition.columns.values:
                                                    if list_sms[1]['value_operator'] == '!=':
                                                        df_condition.loc[df_condition[column].str[list_sms[1]['value_start_position'] - skip_character:list_sms[1]['value_end_position']] != list_sms[1]['value'],column] = df_condition[column]
                                                    if list_sms[1]['value_operator'] == '==':
                                                        df_condition.loc[df_condition[column].str[list_sms[1]['value_start_position'] - skip_character:list_sms[1]['value_end_position']] == list_sms[1]['value'],column] = df_condition[column]
                                                    count_df_condition = df_condition[column].count()
                                            else:
                                                df_condition = df_condition.join(self.visa_apply_condition(select_df_tmp,column,list_condition,'|',skip_character,skip_columns),how='right')
                                                count_df_condition = df_condition[column].count()
                                            df_column = df_column.join(df_condition[list_base],how='right')

                                            if count_df_condition>0:
                                                df_column[name_column] = df_condition[column].str[start_position_column:end_position_column]
                                                count_data_column += 1
                                            else:
                                                df_column[name_column] = nan
                                        else:
                                            df_column = df_column.join(select_df_tmp[list_base],how='right')

                                            df_column[name_column] = select_df_tmp[column].str[start_position_column:end_position_column]
                                            count_data_column += 1
                                        if count_data_column>0:
                                            dfnew = pd.merge(dfnew,df_column,how='outer')
                                            df_column = df_column[df_column[name_column].notna()]
                                            count_data_tc+=1
                                        break
                if count_data_tc > 0:
                    dfnew = dfnew.rename(columns = {'0':'app_id','1':'app_customer_code','2':'app_type_file','3':'app_hash_file','4':'app_processing_date'})
                    str_columns = '"' + '","'.join(map(str,list(dfnew.columns.values))) + '"'
                    dfnew.to_csv(filename_record,index=False,encoding='utf-8')
                    self.s3.upload_object(bucket,filename_record,f"adapters/{filename_record}",)
                    customer_code = str(self.customer_code).lower()
                    i_dqp= dq_p.dq_profilling(self.path_yml,self.customer_code,self.log_name,self.module)
                    df_str = dfnew.astype(str)
                    flg_check, rows_nopassed ,lst_index_obs = i_dqp.dq_pf_casting_columns_type(df_str,"VISA",table_name_stg)
                    if flg_check == False:
                        log.logs().exist_file( "OPERATIONAL",
                            self.customer_code, 
                            "VISA",
                            self.log_name,
                            "INTERCHANGE OF VISA: DQ-PROFILING", 
                            "INFO", 
                            "Shape of trx structured(raw) before it be evaluated for Data Quality process : " + str(df_new.shape),
                            self.module)
                        
                        df_observados_1 =  i_dqp.dq_pf_df_observed(df_new,rows_nopassed,lst_df_obs= lst_index_obs)
                        df_observados = df_observados_1[['app_id', 'app_type_file', 'app_customer_code', 'app_hash_file', 'transaction_code', 'app_processing_date', 'account_number','message_error']]
                        i_dc = dq_clean.dq_cleaning(self.customer_code,self.log_name,self.module)
                        df_new = i_dc.dq_cls_omitir_obs(df_new,lst_index_obs)
                        cnn = con.connect_to_postgreSQL()
                        table_obs= "obs_raw_visa_transactions_hm"
                        
                        mssg = cnn.insert_from_df(df_observados,self.schema,table_obs,'append')
                        log.logs().exist_file(
                            "OPERATIONAL",
                            self.customer_code, 
                            "VISA",
                            self.log_name,
                            "INTERCHANGE OF VISA: DQ-CLEANSING", 
                            "INFO", 
                            "Inserted " + str(mssg) +" observed records",
                            self.module
                        )
                    result = self.ps.insert_from_csv(
                        f'{table_name}_{customer_code}',
                        """(format csv, header true, quote '"' )""",
                        bucket,
                        f"adapters/{filename_record}",
                        str_columns,
                        False
                    )
                    result_message = f"{row['type_record']} : {len(dfnew)} {result}"
                else:
                    result_message = f"{row['type_record']} : data not found"
                log.logs().exist_file(
                    "OPERATIONAL",
                    self.customer_code,
                    "VISA",
                    self.log_name,
                    "ADAPTER OF VISA FILE",
                    "INFO",
                    result_message,
                    self.module
                )
            return 'finished'
        except Exception as e:   
            log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA",
                self.log_name,
                "ADAPTER OF VISA FILE",
                "ERROR",
                "error loading adapter file :" + hash_file
                + " | exception reading file: "
                + str(e),
                self.module
            )
              
    def mastercard_extract_pds(self, str_value : str, find_str : int,log_name: str = None, client:str = None) -> str:
        """Method to extract pds of Dataelements

        Args:
            str_value (str): value of data element
            find_str (str): pds number

        Returns:
            str: pds value
        """

        
        if str_value[0:4].isnumeric():
            len_str = len(str_value)
            len_value = int(str_value[4:7])
            str_pds = int(str_value[0:4])
            len_max_value = len_value + 7
            
            if str_pds == find_str:
                return str_value[7:len_max_value]
            elif len_max_value < len_str:
                return self.mastercard_extract_pds(str_value[len_max_value:],find_str)
            else:
                return ''
        else:
            return ''

    def mastercard_find_pds(self, df_pds : pd.DataFrame, list_column_pds: list,column_name_de: str,log_name: str = None, client:str = None)-> pd.DataFrame:
        """Method to find pds of Dataelements

        Args:
           df_pds(dataframe) : Dataframe with data
           list_column_pds (list): list of pds names
           column_name_de (str): column name of data element

        Returns:
            pd.DataFrame: dataframe with DE data
        """
        list_pds = []
        for value in df_pds[column_name_de].values:
            dict_pds = {}
            dict_pds.update({'de' : column_name_de[2:]})
            for pds_value in list_column_pds:
                if value!=None:
                    dict_pds.update({pds_value : self.mastercard_extract_pds(value,int(pds_value))})
                else:
                    dict_pds.update({pds_value : ''})
            list_pds.append(dict_pds)
        return pd.DataFrame(list_pds)
    
    def mastercard_upload_adapter(self, filename_parquet: str, file_type: str = 'in',hash_file:str=None,number_file:str = None, string_date:str=None) -> str:
        """Load Mastercard adapter based on type as csv object
        
        Args:
            filename_parquet (str): local parquet file to read
            file_type (str): type of file 
            hash_file (str): hash code of file
            number_file (str): number of file in queue
            string_date (str): date of file

        Returns:
            str: Message

        """
        module = 'ADAPTER'
        log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            "loading mastercard adapter type file : " + file_type,
            self.module
        )

        bucket = self.structured
        table_name_adapter = 'control.t_mastercard_adapter'
        path_yml = "Module/DataQuality/config/dq_profilling_tipodato.yml"
        string_date = datetime.now().strftime('%Y%m%d')
        records_adapter = self.ps.select(table_name_adapter,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)")
        df = pd.read_parquet(filename_parquet,engine='pyarrow')
        df['app_id'] = df.index + 1
        df['app_type_file'] = file_type.upper()
        filename = 'adapter_mastercard.csv'
        customer = self.customer_code
        table_name_base = 'operational.stg_adapter_mastercard'
        list_columns_de = []
        list_columns_pds = []
        list_columns_base = ['app_id','app_type_file','app_client','app_hash','MESSAGE_TYPE','app_file_date']
        list_columns_base_pds = ['app_id','app_type_file','app_client','app_hash','app_file_date']
        dict_adapter_de = []
        list_adapter = []
        start_position_column = 0
        start_position_column_subfield = 0
        pds_start_position_column_subfield = 0
        pds_code = 2
        df_adapter = pd.DataFrame(records_adapter)
        df_adapter_de = pd.DataFrame()
        df_adapter['length'] = df_adapter['field_max_length']
        df_adapter.loc[df_adapter['field_max_length'] == 0, 'length'] = df_adapter['field_min_length']

        df_adapter.loc[df_adapter['type_record'] != pds_code, 'list_column_de'] = 'DE' + df_adapter['de'].astype(str)
        list_columns_de.extend(list_columns_base)
        list_columns_de.extend(df_adapter[df_adapter['list_column_de'].notna()]['list_column_de'].drop_duplicates().values)
        df = df.filter(items=list_columns_de)
        df_adapter_pds = df_adapter[df_adapter['pds'] == pds_code].reset_index(drop=True)
        table_name_pds = table_name_base + '_' + df_adapter_pds['type_record'][0]
        for type_record in df_adapter['type_record'].drop_duplicates().values:
            df_new = pd.DataFrame()
            df_adapter_record = pd.DataFrame()
            select_df = pd.DataFrame()
            df_load = pd.DataFrame()
            filename_record = f"FILES/ADAPTERS/{customer}_{file_type}_{number_file}_{type_record}_{filename}"
            start_position_column_subfield = 0
            len_column_subfield = 0
            count = 0
            table_name = table_name_base + '_' + type_record
            df_adapter_record = df_adapter_record.join(df_adapter[(df_adapter['type_record']==type_record) & (df_adapter['pds']<pds_code)].sort_values(by=['de','subfield'],ascending=True).reset_index(drop=True),how='right')
            list_row = df_adapter_record['message_type_identifier'].drop_duplicates().values[0].split(',')
            select_df = select_df.join(df[df['MESSAGE_TYPE'].str.startswith(tuple(list_row), na=False)].reset_index(drop=True),how='right')
            df_new = df_new.join(select_df[list_columns_base],how='right')
            for index,column_name in enumerate(df_adapter_record['column_name'].values):
                if select_df.empty:
                    continue

                len_column = 0
                if df_adapter_record['subfield'][index] == 0: 
                    len_column = df_adapter_record['field_max_length'][index] if int(df_adapter_record['field_max_length'][index]) > 0 else df_adapter_record['field_min_length'][index]
                    start_position_column_subfield = 0
                    start_position = 0
                    df_fields_series = select_df[df_adapter_record['list_column_de'][index]].str[
                                       start_position:len_column].str.replace('\r', '')

                elif df_adapter_record['field_name'][index] == 'Card Acceptor Name/Location':
                    column_de_name = df_adapter_record['list_column_de'][index]
                    acceptor_split_df = select_df[df_adapter_record['list_column_de'][index]]\
                        .str.split('\\', 3, expand=True)
                    acceptor_split_last = acceptor_split_df[len(acceptor_split_df.columns) - 1]
                    field_length = int(df_adapter_record['subfield_min_length'][index])

                    if df_adapter_record['subfield_name'][index] == 'Card Acceptor Country Code':
                        df_fields_series = acceptor_split_last.str[-3:].rename(column_de_name)

                    elif len(acceptor_split_df.columns) < 3:
                        log.logs().exist_file(
                            "OPERATIONAL",
                            self.customer_code,
                            "MASTERCARD",
                            self.log_name,
                            "ADAPTER OF MASTERCARD FILE",
                            "WARNING",
                            'cannot extract subfields from Card Acceptor Name/Location',
                            self.module
                        )

                    elif df_adapter_record['subfield_name'][index] == 'Card Acceptor Name':
                        df_fields_series = acceptor_split_df[0].str[:field_length].rename(column_de_name)

                    elif df_adapter_record['subfield_name'][index] == 'Card Acceptor Street Address':
                        df_fields_series = acceptor_split_df[1].str[:field_length].rename(column_de_name)

                    elif df_adapter_record['subfield_name'][index] == 'Card Acceptor City':
                        df_fields_series = acceptor_split_df[2].str.replace('\\', '').str[:field_length].rename(column_de_name)

                    elif df_adapter_record['subfield_name'][index] == 'Card Acceptor Postal (ZIP) Code':
                        df_fields_series = acceptor_split_last.str[:-6].str.replace('\\', '').str[:field_length].rename(column_de_name)

                    elif df_adapter_record['subfield_name'][index] == 'Card Acceptor State, Province, or Region Code':
                        df_fields_series = acceptor_split_last.str[-6:-3].rename(column_de_name)

                    else:
                        df_fields_series = acceptor_split_last.str[:field_length]
                else:
                    if df_adapter_record['subfield'][index] == 1:
                        start_position_column_subfield = 0
                    len_column_subfield = df_adapter_record['subfield_max_length'][index] if int(df_adapter_record['subfield_max_length'][index]) > 0 else df_adapter_record['subfield_min_length'][index]
                    start_position = start_position_column_subfield
                    len_column = start_position + len_column_subfield
                    start_position_column_subfield += len_column_subfield
                    df_fields_series = select_df[df_adapter_record['list_column_de'][index]].str[
                                       start_position:len_column].str.replace('\r', '')

                df_load = pd.concat([df_load, df_fields_series], axis=1)
                df_load = df_load.rename(columns={df_adapter_record['list_column_de'][index]: column_name})

                if df_adapter_record['pds'][index] == 1:
                    pds_start_position_column_subfield = 0
                    pds_len_column_subfield = 0
                    df_adapter_pds = df_adapter_pds.sort_values(by=['de','subfield'],ascending=True).reset_index(drop=True)
                    list_columns_pds = df_adapter_pds['de'].drop_duplicates().values
                    df_load_sub = pd.DataFrame()
                    df_load_sub = pd.concat([df_load_sub,self.mastercard_find_pds(select_df,list_columns_pds,df_adapter_record['list_column_de'][index])],axis=1)
                    for pds_index, pds_column_name in enumerate(df_adapter_pds['column_name'].values):
                        df_column_pds = pd.DataFrame()
                        if not df_load_sub[df_load_sub[df_adapter_pds['de'][pds_index]] !=''].empty:
                            pds_len_column = 0
                            if df_adapter_pds['subfield'][pds_index] == 0: 
                                pds_len_column = df_adapter_pds['field_max_length'][pds_index] if int(df_adapter_pds['field_max_length'][pds_index]) > 0 else df_adapter_pds['field_min_length'][pds_index]
                                pds_start_position_column_subfield = 0
                                pds_start_position = 0
                            else:
                                if df_adapter_pds['subfield'][pds_index] == 1:
                                    pds_start_position_column_subfield = 0
                                pds_len_column_subfield = df_adapter_pds['subfield_max_length'][pds_index] if int(df_adapter_pds['subfield_max_length'][pds_index]) > 0 else df_adapter_pds['subfield_min_length'][pds_index]
                                pds_start_position = pds_start_position_column_subfield
                                pds_len_column = pds_start_position + pds_len_column_subfield
                                pds_start_position_column_subfield += pds_len_column_subfield
                            df_load = pd.concat([df_load,df_load_sub[df_adapter_pds['de'][pds_index]].str[pds_start_position:pds_len_column]],axis=1)
                            df_load = df_load.rename(columns = {df_adapter_pds['de'][pds_index] : pds_column_name})
                count+=1
            if count>0:
                df_new = pd.concat([df_new,df_load],axis=1)
                df_new = df_new.rename(columns = {'app_client':'app_customer_code','app_hash':'app_hash_file','MESSAGE_TYPE':'app_message_type','app_file_date' : 'app_processing_date'})
                str_columns = '"' + '","'.join(map(str,list(df_new.columns.values))) + '"'
                i_dqp= dq_p.dq_profilling(self.path_yml,self.customer_code,self.log_name,self.module)
                df_str = df_new.astype(str)
                flg_check, rows_nopassed ,lst_index_obs = i_dqp.dq_pf_casting_columns_type(df_str,"MC","dq_trx_detail_MC")
                if flg_check == False:
                    log.logs().exist_file( "OPERATIONAL",
                        self.customer_code, 
                        "MASTERCARD",
                        self.log_name,
                        "INTERCHANGE OF MC: DQ-PROFILING", 
                        "INFO", 
                        "Shape of trx structured(raw) before it be evaluated for Data Quality process : " + str(df_new.shape),
                        self.module)
                    
                    df_observados_1 =  i_dqp.dq_pf_df_observed(df_new,rows_nopassed,lst_df_obs= lst_index_obs)
                    df_observados = df_observados_1[['app_id', 'app_type_file', 'app_customer_code', 'app_hash_file', 'app_message_type', 'app_processing_date', 'pan','message_error']]
                    i_dc = dq_clean.dq_cleaning(self.customer_code,self.log_name,self.module)
                    df_new = i_dc.dq_cls_omitir_obs(df_new,lst_index_obs)
                    cnn = con.connect_to_postgreSQL()
                    table_obs= "obs_raw_mastercard_data_element_hm"
                    schema= "data_review"
                    mssg = cnn.insert_from_df(df_observados,schema,table_obs,'append')
                    log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code, 
                        "MASTERCARD",
                        self.log_name,
                        "INTERCHANGE OF MC: DQ-CLEANSING", 
                        "INFO", 
                        "Inserted " + str(mssg) +" observed records",
                        self.module
                    )
                if df_new.shape[0]>0:
                    df_new.to_csv(filename_record,index=False, encoding='utf-8', quoting=csv.QUOTE_ALL)
                    self.s3.upload_object(bucket,filename_record,f"adapters/{filename_record}")
                    customer_code = str(self.customer_code).lower()
                    result = self.ps.insert_from_csv(f'{table_name}_{customer_code}', """(format csv, header true, quote '"' )""", bucket,f"adapters/{filename_record}",str_columns,False)
                    result_message = f"{type_record} : {len(df_new)} {result}"
                else:
                    result_message = f"{type_record} : data excluded from the data quality process for not meeting quality rules."
            if count==0:
                result_message = f"{type_record} : data not found"
            log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "MASTERCARD",
                self.log_name,
                "ADAPTER OF MASTERCARD FILE",
                "INFO",
                result_message,
                self.module
            )
        return 'finished'
    
    def visa_clear_stg(self):
        """Clean Visa staging tables for execution, returns message"""

        string_date = datetime.now().strftime('%Y%m%d')
        table_name_adapter = 'control.t_visa_adapter'
        table_name_stg_base = 'operational.stg_adapter_visa_transaction'
        records_adapter = self.ps.select(table_name_adapter,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)")
        df_records = pd.DataFrame(records_adapter)
        for value in df_records['type_record'].drop_duplicates().reset_index(drop=True).values:
            table_name = table_name_stg_base
            if value != 'transaction':
                table_name += '_'+value
            customer_code = str(self.customer_code).lower()
            table_name = f'{table_name}_{customer_code}'
            message = self.ps.truncate_table(table_name)

            log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code,
                        "VISA",
                        self.log_name,
                        "ADAPTER OF VISA FILE",
                        "INFO",
                        message,
                        self.module
                    )
        return 'finished'
    
    def mastercard_clear_stg(self):
        """Clean Mastercard staging tables for execution, returns message"""
        string_date = datetime.now().strftime('%Y%m%d')
        table_name_adapter = 'control.t_mastercard_adapter'
        table_name_stg_base = 'operational.stg_adapter_mastercard'
        records_adapter = self.ps.select(table_name_adapter,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)")
        df_records = pd.DataFrame(records_adapter)
        for value in df_records['type_record'].drop_duplicates().reset_index(drop=True).values:
            customer_code = str(self.customer_code).lower()
            table_name = f'{table_name_stg_base}_{value}_{customer_code}'
            message = self.ps.truncate_table(table_name)
            log.logs().exist_file(
                    "OPERATIONAL",
                    self.customer_code,
                    "MASTERCARD",
                    self.log_name,
                    "ADAPTER OF MASTERCARD FILE",
                    "INFO",
                    message,
                    self.module
                )
        return 'finished'

class get_others:
    """Class for other methods relationated to adpaters
    
    Params:
        customer_code (str): customer code
        log_name (str): name of logs
    """
    def __init__(self, customer_code: str, log_name: str):
   
        load_dotenv()
        self.ps = con.connect_to_postgreSQL()
        self.s3 = con.connect_to_s3()
        self.structured = os.getenv("STRUCTURED_BUCKET")
        self.log = os.getenv("LOG_BUCKET")
        self.customer_code = customer_code
        self.log_name = log_name
        self.module = 'ADAPTER'
        self.region_country_global='9'
        self.debug = os.getenv("ENV_DEBUG")
        log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                "Mode debug : "
                + self.debug,
                self.module
            )

    
    def update_column_table_from_adapter(self, adapter_column_base:list, adapter_table_scheme:str, adapter_table_name: str, table_scheme: str, table_name: str, type_record: str, column_type: bool = False, column_format: bool = False, column_partition: str = None, partition_type: str = None, adapter_where: str = None, adapter_order: str = None,log_name: str = None, client:str = None)->str:
        """Update columns from the adapter
        
        Args:
            adapter_column_base (list): List of columns for adapter
            adapter_table_scheme (str): adapter schema for table
            adapter_table_name (str): adapter table name
            table_scheme (str): temporal table schema
            table_name (str): temporal table name
            type_record (str): type of record
            column_type (bool): column type indicator
            column_format (bool): column format indicator
            column_partition (str): column for partition
            partition_type (str): type of partition
            adapter_where (str): condition for adapter query 
            adapter_order (str): column order by for adapter query

        Returns:
            result_message (str): Message
        """
        module = 'ADAPTER'
        adapter_table = adapter_table_scheme+'.'+adapter_table_name
        adapter_columns_name = 'column_name, length, column_type'
        list_table_y = []
        list_table_x = []
        if adapter_where==None:
            adapter_where=''
        if adapter_order==None:
            adapter_order=''
        if column_format:
            adapter_columns_name = " replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(column_name,']',''),'[',''),'.',''),'''',''),')',''),'(',''),'/',''),'-','_'),' ','_'),'___','_'),'__','_') as column_name,length,column_type,column_decimal"
        list_table_y.extend(adapter_column_base)
        list_table_x = self.ps.get_structure_table_from_db(table_scheme,table_name)
        list_table_y.extend(self.ps.select(adapter_table,f"where type_record = '{type_record}' {adapter_where} {adapter_order}",adapter_columns_name))
        df_x = pd.DataFrame(list_table_x)
        df_y = pd.DataFrame(list_table_y)
        if not self.ps.table_exists(table_scheme+'.'+table_name):
            self.ps.create_table(list_table_y,table_scheme+'.'+table_name,column_type,column_partition,partition_type)  

            log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                "table created : "
                + table_scheme
                + "."
                + table_name,
                self.module
            )

            return self.update_column_table_from_adapter(adapter_column_base,adapter_table_scheme,adapter_table_name,table_scheme,table_name,type_record,column_format=column_format,adapter_where=adapter_where,adapter_order=adapter_order)
        if len(df_x) != len(df_y):
            list_table_difference = []
            for index, value in enumerate(df_y['column_name'].values):
                if not value in (df_x['column_name'].values):
                    list_table_difference.append({'column_name': df_y['column_name'][index], 'length' : df_y['length'][index], 'column_type': df_y['column_type'][index]})
            result_message = self.ps.add_column(list_table_difference,table_scheme,table_name)
            log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                result_message,
                self.module
            )
            if len(df_x)<=len(df_y):
                return self.update_column_table_from_adapter(adapter_column_base,adapter_table_scheme,adapter_table_name,table_scheme,table_name,type_record,column_format=column_format,adapter_where=adapter_where,adapter_order=adapter_order)
            result_message = f'columns difference to table : {table_scheme}.{table_name}'
        if len(df_x) == len(df_y):
            list_table_difference = []
            for index, value in enumerate(df_y['column_name'].values):
                if value in (df_x['column_name'].values):
                    if str(df_x[df_x['column_name'] == df_y['column_name'][index]]['data_type'].values[0]).replace('character varyinresult_messageg','varchar') != df_y['column_type'][index]:
                        list_table_difference.append({'column_name': df_y['column_name'][index], 'length' : df_y['length'][index], 'column_type': df_y['column_type'][index]})
            result_message = f'{len(list_table_difference)} columns difference to table : {table_scheme}.{table_name}'
        return result_message

    def visa_config_table_adapter_dh(self, string_start_date : str = None, string_end_date : str = None,log_name: str = None, client:str = None) ->str:
        """Configuration of the visa adapter table dh
        
        Args:
           string_start_date (str): starting date for range  
           string_end_date (str): ending date for range

        Returns:
            str: Message
        """
        adapter_table_scheme = 'control'
        adapter_table_name = 't_visa_adapter'
        adapter_table = adapter_table_scheme+'.'+adapter_table_name
        adapter_column_base = [{'column_name': 'app_id','length': '10', 'column_type': 'numeric'},{'column_name': 'app_customer_code','length': '50', 'column_type': 'text'},{'column_name': 'app_type_file','length': '50', 'column_type': 'text'},{'column_name': 'app_hash_file','length': '300', 'column_type': 'text'},{'column_name': 'app_processing_date','length': '30', 'column_type': 'date'}]
        if string_start_date == None:
            string_start_date = datetime.now().strftime('%Y%m%d')
        if string_end_date == None:
            string_end_date = string_start_date
        customer_table = 'control.t_customer'
        list_type_record = self.ps.select(adapter_table,f"where to_date('{string_start_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)",'distinct type_record')
        list_customer = self.ps.select(customer_table,"where status = 'ACTIVE'",'name, code')
        for value in list_type_record:
            table_scheme = 'operational'
            table_name = 'dh_visa_transaction'
            if value['type_record']!='transaction':
                table_name += '_'+value['type_record']
            result_message = self.update_column_table_from_adapter(adapter_column_base,adapter_table_scheme,adapter_table_name,table_scheme,table_name,value['type_record'],True,True,'app_customer_code','list',adapter_order=' order by type_record,tcr,position,column_name')
            customer_code = self.customer_code
            partition_name = table_name+'_'+customer_code
            message_partition = self.ps.create_table_partition_list(table_scheme,table_name,table_scheme,partition_name,f"'{customer_code}'",'app_type_file','list')
        
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_partition),
            self.module
            )
                     
            message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_in',f"'IN'",'app_processing_date','range')
            
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_partition),
            self.module
            )          
                            
            message_partition = self.ps.create_table_partition_range(table_scheme,partition_name+'_in',table_scheme,partition_name+'_in',string_start_date,string_end_date)
            
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_partition),
            self.module
            )                        
            
            message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_out',f"'OUT'",'app_processing_date','range')
            
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_partition),
            self.module
            )                   
            
            message_partition = self.ps.create_table_partition_range(table_scheme,partition_name+'_out',table_scheme,partition_name+'_out',string_start_date,string_end_date)
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_partition),
            self.module
            )      
            message_index = self.ps.create_table_index(table_scheme,table_name,table_name+'_idx','app_id,app_hash_file')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_index),
            self.module
            )
            message_index = self.ps.create_table_index(table_scheme,table_name,table_name+'_date_idx','app_processing_date')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_index),
            self.module
            )
            
        message_index = self.ps.create_table_index(table_scheme,'dh_visa_transaction','dh_visa_transaction_account_number_idx','account_number')
        log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_index),
            self.module
            )          
        
        message_create_table_index = self.ps.create_table_index(table_scheme,'dh_visa_transaction_sms','dh_visa_transaction_sms_card_number_idx','card_number')
        
        log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_create_table_index),
            self.module
            )        
    
        return 'finished'
    def visa_config_table_adapter_stg(self, string_date : str = None,log_name: str = None, client:str = None) ->str:
        """Configuration of the visa adapter table staging
        
        Args:
           string_date (str): date for range  

        Returns:
            str: Message
        
        """
        adapter_table_scheme = 'control'
        adapter_table_name = 't_visa_adapter'
        adapter_table = adapter_table_scheme+'.'+adapter_table_name
        adapter_column_base = [{'column_name': 'app_id','length': '10', 'column_type': 'numeric'},{'column_name': 'app_customer_code','length': '50', 'column_type': 'text'},{'column_name': 'app_type_file','length': '50', 'column_type': 'text'},{'column_name': 'app_hash_file','length': '300', 'column_type': 'text'},{'column_name': 'app_processing_date','length': '30', 'column_type': 'text'}]
        table_scheme = 'operational'
        table_name_base = 'stg_adapter_visa_transaction'
        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')
        customer_table = 'control.t_customer'
        list_type_record = self.ps.select(adapter_table,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)",'distinct type_record')
        list_customer = self.ps.select(customer_table,"where status = 'ACTIVE'",'name, code')
        for value in list_type_record:
            table_name = table_name_base

            if value['type_record']!='transaction':
                table_name += '_'+value['type_record']             
            
            message_update = self.update_column_table_from_adapter(adapter_column_base,adapter_table_scheme,adapter_table_name,table_scheme,table_name,value['type_record'],False,False,'app_customer_code','list',adapter_order=' order by type_record,tcr,position,column_name')
                         
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_update),
            self.module
            )                      
            
            customer_code = self.customer_code

            partition_name = table_name+'_'+customer_code
            message_partition = self.ps.create_table_partition_list(table_scheme,table_name,table_scheme,partition_name,f"'{customer_code}'",'app_type_file','list')

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_partition),
            self.module
            )      
            message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_in',f"'IN'")
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_partition),
            self.module
            )      
            message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_out',f"'OUT'")

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(message_partition),
            self.module
            )      


        return 'finished'
    def mastercard_config_table_adapter_stg(self, string_date : str = None,log_name: str = None, client:str = None)->str:
        """Configuration of the mastercard adapter table staging
        
        Args:
           string_date (str): date for range  

        Returns:
            str: Message
        
        """
        adapter_table_scheme = 'control'
        adapter_table_name = 't_mastercard_adapter'
        adapter_table = adapter_table_scheme+'.'+adapter_table_name
        adapter_column_base = [{'column_name': 'app_id','length': '10', 'column_type': 'numeric'},{'column_name': 'app_customer_code','length': '50', 'column_type': 'text'},{'column_name': 'app_type_file','length': '50', 'column_type': 'text'},{'column_name': 'app_hash_file','length': '300', 'column_type': 'text'},{'column_name': 'app_message_type','length': '10', 'column_type': 'text'},{'column_name': 'app_processing_date','length': '30', 'column_type': 'text'}]
        table_scheme = 'operational'
        table_name_base = 'stg_adapter_mastercard'
        module = 'ADAPTER'

        pds_code = '2'
        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')
        customer_table = 'control.t_customer'
        list_type_record = self.ps.select(adapter_table,f"where pds <{pds_code} and to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)",'distinct type_record')
        list_customer = self.ps.select(customer_table,"where status = 'ACTIVE'",'name, code')
        for value in list_type_record:
            table_name = table_name_base + '_' + value['type_record']        
                 
            result_message = self.update_column_table_from_adapter(adapter_column_base,adapter_table_scheme,adapter_table_name,table_scheme,table_name,value['type_record'],False,False,'app_customer_code','list',adapter_order=' order by pds,de')
               
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(result_message),
            self.module
            )             
           
            customer_code = self.customer_code

            partition_name = table_name+'_'+customer_code
            message_partition = self.ps.create_table_partition_list(table_scheme,table_name,table_scheme,partition_name,f"'{customer_code}'",'app_type_file','list')

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(message_partition),
            self.module
            )         

            message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_in',f"'IN'")

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(message_partition),
            self.module
            )         

            message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_out',f"'OUT'")
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(message_partition),
            self.module
            )         

        return 'finished'
    def mastercard_config_table_adapter_dh(self, string_start_date : str = None, string_end_date : str = None,log_name: str = None, client:str = None) ->str:
        """Configuration of the mastercard adapter table dh
        
        Args:
            string_start_date (str): starting date for range  
            string_end_date (str): ending date for range 

        Returns:
            str: Message
        
        """
        adapter_table_scheme = 'control'
        adapter_table_name = 't_mastercard_adapter'
        adapter_table = adapter_table_scheme+'.'+adapter_table_name
        adapter_column_base = [{'column_name': 'app_id','length': '10', 'column_type': 'numeric'},{'column_name': 'app_customer_code','length': '50', 'column_type': 'text'},{'column_name': 'app_type_file','length': '50', 'column_type': 'text'},{'column_name': 'app_hash_file','length': '300', 'column_type': 'text'},{'column_name': 'app_message_type','length': '10', 'column_type': 'text'},{'column_name': 'app_processing_date','length': '30', 'column_type': 'date'}]
        module = 'ADAPTER'
        """log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        "Operational table configuration: "
        + adapter_table_scheme
        + "."
        + adapter_table_name,
        self.module
        )   """
          
        if string_start_date == None:
            string_start_date = datetime.now().strftime('%Y%m%d')
        if string_end_date == None:
            string_end_date = string_start_date
        customer_table = 'control.t_customer'
        list_type_record = self.ps.select(adapter_table,f"where to_date('{string_start_date}','yyyymmdd') between start_date and coalesce(end_date,current_date) and type_record <>'private_data_subelement'",'distinct type_record')
        list_customer = self.ps.select(customer_table,"where status = 'ACTIVE'",'name, code')
        for value in list_type_record:
            table_scheme = 'operational'
            table_name = 'dh_mastercard'
            table_name += '_'+value['type_record']
            result_message = self.update_column_table_from_adapter(adapter_column_base,adapter_table_scheme,adapter_table_name,table_scheme,table_name,value['type_record'],True,True,'app_customer_code','list',adapter_order=' order by pds,de')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(result_message),
            self.module
            )         
            customer_code = self.customer_code

            partition_name = table_name+'_'+customer_code
            message_partition = self.ps.create_table_partition_list(table_scheme,table_name,table_scheme,partition_name,f"'{customer_code}'",'app_type_file','list')

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(message_partition),
            self.module
            )  

            message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_in',f"'IN'",'app_processing_date','range')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(message_partition),
            self.module
            )  


            message_partition = self.ps.create_table_partition_range(table_scheme,partition_name+'_in',table_scheme,partition_name+'_in',string_start_date,string_end_date)

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(message_partition),
            self.module
            )  




            message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_out',f"'OUT'",'app_processing_date','range')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(message_partition),
            self.module
            )  

            message_partition = self.ps.create_table_partition_range(table_scheme,partition_name+'_out',table_scheme,partition_name+'_out',string_start_date,string_end_date)

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(message_partition),
            self.module
            )
            message_index = self.ps.create_table_index(table_scheme,table_name,table_name+'_idx','app_id,app_hash_file')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(message_index),
            self.module
            )
            message_index = self.ps.create_table_index(table_scheme,table_name,table_name+'_date_idx','app_processing_date')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(message_index),
            self.module
            )

        message_index = self.ps.create_table_index(table_scheme,'dh_mastercard_data_element','dh_mastercard_data_pan_idx','pan')
        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        str(message_index),
        self.module
        ) 
        return 'finished'

    def visa_load_transaction(self, string_date: str = None,type_file: str = None, hash_files: list = [])->str:
        """Load of visa transaction
        
        Args:
            string_date (str): date for range condition  

        Returns:
            str: Message
        
        """
        adapter_table_scheme = 'control'
        adapter_table_name = 't_visa_adapter'
        adapter_table = adapter_table_scheme+'.'+adapter_table_name
        customer_code = self.customer_code
        module = 'ADAPTER'
        hash_file = '45b9c08a79b0a9ffc3803433b6cf241b86acb2aa94c78f51fe7147ed18a1423b'
        number_file = '1'
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        "loading transactions type file : "+ type_file,
        self.module
        )   
        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')

        app_processing_date = datetime.strptime(string_date, '%Y%m%d').strftime('%Y-%m-%d')
        hash_files_in = "'{}'".format("','".join(hash_files))
        list_type_record = self.ps.select(adapter_table,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)",'distinct type_record')
        df_adapter = pd.DataFrame(self.ps.select(adapter_table,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)"))
        for value in df_adapter['type_record'].drop_duplicates().values:
            table_scheme = 'operational'
            table_name = 'dh_visa_transaction'
            table_name_stg = 'stg_adapter_visa_transaction'
            if value!='transaction':
                table_name += '_'+value
                table_name_stg += '_'+value
            df_structure_dh = pd.DataFrame(self.ps.get_structure_table_from_db(table_scheme,table_name,'order by column_name'))
            df_structure_stg = pd.DataFrame(self.ps.get_structure_table_from_db(table_scheme,table_name_stg,'order by column_name'))
            select_query = 'select '
            left_query = ''
            str_column_dh = ''
            has_app_processing_date = False
            for column in df_structure_stg['column_name'].values:
                if column in ('app_creation_user','app_creation_date'):
                    continue
                str_column_format = column.replace(']','').replace('[','').replace('.','').replace("'",'').replace(')','').replace('(','').replace('/','').replace('-','_').replace(' ','_').replace('___','_').replace('__','_') + ','
                str_column_dh += str_column_format
                column = f'"{column}"'
                if column == '"app_id"':
                    column ='a.app_id::numeric'
                elif column in ('"app_customer_code"','"app_type_file"','"app_hash_file"','"app_creation_user"','"app_creation_date"'):
                    column = 'a.'+column.replace('"','')
                elif column == '"app_processing_date"':
                    column = f"to_date(app_processing_date,'yyyy-mm-dd')"
                    has_app_processing_date = True
                elif column == '"account number"':
                    column_str = column.replace('"','')
                    column = f"replace({column},'*','0')::{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_type'].values[0]}"
                elif column == '"account number extension"':
                    column_str = column.replace('"','')
                    column = f"""
                    case
                        when replace({column},' ','') = '' then NULL
                        else replace({column},'*','0')::{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_type'].values[0]}
                    end        
                    """
                elif column == '"account reference number date"':
                    column = f"""to_date(left(app_processing_date,4)||right({column},3),'yyyyddd')"""
                elif column == '"purchase date"':
                    column = f"""
                    case 
                        when {column} <> '0000' then 
                            case 
                                when left({column},2)::integer<=substr(app_processing_date,6,2)::integer then to_date(left(app_processing_date,4)||{column},'yyyymmdd')
                                else to_date((left(app_processing_date,4)::integer - 1)::varchar||{column},'yyyymmdd')
                            end
                    end
                    """
                elif column == '"destination amount"':
                    column_str = column.replace('"','')
                    column = f"{column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]})"
                elif column == '"source amount"':
                    column_str = column.replace('"','')
                    column = f"{column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]})"
                elif column == '"central processing date"':
                    column = f"to_date(left(app_processing_date,4)||right({column},3),'yyyyddd')"
                elif column == '"national reimbursement fee"':
                    column_str = column.replace('"','')
                    column = f"""
                    case    
                        when regexp_replace({column},'[^0-9]+', '', 'g') != {column} then NULL
                        else {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]})
                    end    
                    """
                elif column == '"conversion date"':
                    column = f"""
                    case 
                        when {column} <> '0000' then 
                            case 
                                when right({column},3)::integer<=right(to_char(to_date(app_processing_date,'yyyy-mm-dd'),'yyyyddd'),3)::integer then to_date(left(app_processing_date,4)||right({column},3),'yyyyddd')
                                else to_date((left(app_processing_date,4)::integer - 1)::varchar||right({column},3),'yyyyddd')
                            end
                    end
                    """
                elif column == '"merchant country code"':
                    column = f"trim({column})"
                elif column == '"surcharge amount sp"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"surcharge amount sd"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"surcharge amount in cardholder billing currency sp"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"surcharge amount in cardholder billing currency sd"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"surcharge amount df"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"money transfer foreign exchange fee"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"authorized amount"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"total authorized amount"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"interchange fee amount"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"source currency to base currency exchange rate"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"base currency to destination currency exchange rate"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"optional issuer isa amount"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"local tax"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"national tax"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"other tax"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"settlement date"':
                    column = f"to_date(right({column},5),'yyddd')"
                elif column == '"report date 140"':
                    column = f"to_date(right({column},5),'yyddd')"
                elif column == '"report date 110"':
                    column = f"to_date(right({column},5),'yyddd')"
                elif column == '"credit amount"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"debit amount"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"interchange amount (settlement currency) 140"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"interchange amount (settlement currency) 130"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"clearing amount (clearing currency)"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"interchange value credits (settlement currency)"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"visa charges credits (settlement currency)"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"reimbursement fee credits (settlement currency)"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"visa charges debits (settlement currency)"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"reimbursement fee debits (settlement currency)"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"interchange value debits (settlement currency)"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^c.currency_decimal_separator) end"
                elif column == '"processing date header"':
                    column = f"to_date({column},'yyddd')"
                elif column == '"processing date"':
                    column = f"case when {column} <>'00000' then to_date({column},'yyddd') end"
                elif column == '"destination amount trailer"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"source amount trailer"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"rate table date"':
                    column = f"""
                    case 
                        when {column} is not null then 
                            case 
                                when right({column},3)::integer<=right(to_char(to_date(app_processing_date,'yyyy-mm-dd'),'yyyyddd'),3)::integer then to_date(left(app_processing_date,4)||right({column},3),'yyyyddd')
                                else to_date((left(app_processing_date,4)::integer - 1)::varchar||right({column},3),'yyyyddd')
                            end
                    end
                    """
                elif column == '"local transaction date"':
                    column = f"""
                    case 
                        when {column} is not null then 
                            case 
                                when left({column},2)::integer<=substr(app_processing_date,6,2)::integer then to_date(left(app_processing_date,4)||{column},'yyyymmdd')
                                else to_date((left(app_processing_date,4)::integer - 1)::varchar||{column},'yyyymmdd')
                            end
                    end
                    """
                elif column == '"online settlement date"':
                    column = f"to_date({column},'yymmdd')"
                elif column == '"plus settlement date"':
                    column = f"to_date({column},'mmddyy')"
                elif column == '"reimbursement fee"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"transaction amount"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"filed 54 - original amount - clearing currency"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"transmission date"':
                    column = f"to_date(left(app_processing_date,4)||{column},'yyyymmdd')"
                elif column == '"settlement amount"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"transaction integrity fee"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"surcharge amount sms"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"terminal transaction date"':
                    column = f"to_date({column},'yymmdd')"
                elif column == '"settlement date sms"':
                    column = f"to_date({column},'mmddyy')"
                elif column == '"cardholder billing amount"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"vss processing date"':
                    column = f"to_date({column},'yymmdd')"
                elif column == '"cryptogram amount"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"pre-currency conversion amount"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"cardholder bililng other amount"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"cryptogram cashback amount"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"optional issuer fee - settlement currency"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"optional issuer fee - cardholder billing currency"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"optional issuer isa amount in settlement currency"':
                    column_str = column.replace('"','')
                    column_decimal = df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]
                    column = f"case when operational.isnumeric(operational.eval_amount({column})) then operational.eval_amount({column})::numeric/(10^{column_decimal}) end"
                elif column == '"card number"':
                    column = f"replace({column},'*','0')::numeric"
                else:
                    column_str = column.replace('"','')
                    if df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_type'].values[0] in ['numeric','integer','bigint']:
                        column = f"case when operational.isnumeric({column}) then {column}::{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_type'].values[0]} end"
                    else:
                        column = f"{column}::{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_type'].values[0]}"
                select_query += f' {column},'
            if value in ['vss_110','vss_120','vss_130','vss_140']:
                vss_report_code = value[-3:]
                left_query = f'left join operational.m_currency c on (c.currency_numeric_code = case when operational.isnumeric(a."settlement currency code {vss_report_code}") then a."settlement currency code {vss_report_code}"::integer end)'
            
            str_column_dh = str_column_dh[:-1]
            insert_query = f'insert into {table_scheme}.{table_name}_{customer_code.lower()}_{type_file.lower()}_{string_date} ({str_column_dh}) '
            select_query = select_query[:-1] + f' from {table_scheme}.{table_name_stg}_{customer_code}_{type_file} a '
            query = insert_query + select_query + left_query
            if has_app_processing_date:
                query = f"{query} where app_processing_date = '{app_processing_date}'"
            partition = f"{table_name}_{customer_code}_{type_file}"
            result_message = self.ps.insert(query,[(table_name)])

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            str(result_message),
            self.module
            )
            table_count = self.ps.table_count(
                table_scheme,
                f'{table_name}_{customer_code}_{type_file}_{string_date}',
                f'where app_hash_file in ({hash_files_in})'
            )
            result_message = f'{table_count} {result_message} {table_name}'
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            result_message,
            self.module
            )
        return 'finished'

    def mastercard_load_transaction(self, string_date: str = None,type_file:str=None, hash_files: list = [])->str:
        """Load of mastercard transaction
        
        Args:
            string_date (str): date for range condition  

        Returns:
            str: Message
        
        """
        module = 'ADAPTER'
        adapter_table_scheme = 'control'
        adapter_table_name = 't_mastercard_adapter'
        adapter_table = adapter_table_scheme+'.'+adapter_table_name
        customer_code = self.customer_code
        number_file = '1'
        hash_files_in = "'{}'".format("','".join(hash_files))

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        "loading transactions type file : "+ type_file,
        self.module
        ) 

        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')
        app_processing_date = datetime.strptime(string_date, '%Y%m%d').strftime('%y%m%d')
        list_type_record = self.ps.select(adapter_table,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)",'distinct type_record')
        df_adapter = pd.DataFrame(self.ps.select(adapter_table,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)"))
        for value in df_adapter['type_record'].drop_duplicates().values:
            table_scheme = 'operational'
            table_name = 'dh_mastercard'
            table_name_stg = 'stg_adapter_mastercard'
            table_name += '_'+value
            table_name_stg += '_'+value
            df_structure_dh = pd.DataFrame(self.ps.get_structure_table_from_db(table_scheme,table_name,'order by column_name'))
            df_structure_stg = pd.DataFrame(self.ps.get_structure_table_from_db(table_scheme,table_name_stg,'order by column_name'))

            select_query = 'select '
            left_query = ''
            str_column_dh = ''
            has_app_processing_date = False
            for column in df_structure_stg['column_name'].values:
                if column in ('app_creation_user','app_creation_date'):
                    continue
                str_column_format = column.replace(']','').replace('[','').replace('.','').replace("'",'').replace(')','').replace('(','').replace('/','').replace('-','_').replace(' ','_').replace('___','_').replace('__','_') + ','
                str_column_dh += str_column_format 
                column = f'"{column}"'
                if column == '"app_id"':
                    column ='a.app_id::numeric'
                elif column in ('"app_customer_code"','"app_type_file"','"app_hash_file"','"app_message_type"','"app_creation_user"','"app_creation_date"'):
                    column = 'a.'+column.replace('"','')
                elif column == '"app_processing_date"':
                    column = f"to_date(app_processing_date,'yymmdd')"
                    has_app_processing_date = True
                elif column == '"pan"':
                    column = f"case when operational.isnumeric(replace({column},'*','0')) then replace({column},'*','0')::bigint end"
                elif column == '"amount transaction"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric / POWER(10.0, c.currency_decimal_separator) end"
                elif column == '"amount reconciliation"':
                    column_str = column.replace('"','')                     
                    column = f"case when operational.isnumeric({column}) then {column}::numeric / POWER(10.0, d.currency_decimal_separator) end"
                elif column == '"amount cardholder billing"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric / POWER(10.0, e.currency_decimal_separator) end"
                elif column == '"conversion rate reconciliation"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then substr({column},2)::numeric/(10^left({column},1)::numeric) end"
                elif column == '"conversion rate cardholder billing"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then substr({column},2)::numeric/(10^left({column},1)::numeric) end"
                elif column == '"amount net transaction in reconciliation currency 2"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric / POWER(10.0, d.currency_decimal_separator) end"
                elif column == '"amounts transaction fee 5"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric / POWER(10.0, c.currency_decimal_separator) end"
                elif column == '"amounts transaction fee 7"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric / POWER(10.0, c.currency_decimal_separator) end"
                elif column == '"amount net fee in reconciliation currency 2"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric / POWER(10.0, d.currency_decimal_separator) end"
                elif column == '"date and time local transaction"':
                    column = f"case when operational.isnumeric({column}) then to_timestamp({column},'YYMMDDhh24miss') end"
                elif column == '"date action"':
                    column = f"case when operational.isnumeric({column}) then to_date({column},'yymmdd') end"
                elif column == '"amount currency conversion assessment"':
                    column_str = column.replace('"','')
                    column = f"case when operational.isnumeric({column}) then {column}::numeric/(10^{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_decimal'].values[0]}) end"
                elif column == '"business activity 5"':
                    column = f"case when operational.isnumeric({column}) then to_date({column},'yymmdd') end"
                elif column == '"settlement data 6"':
                    column = f"case when operational.isnumeric({column}) then to_date({column},'yymmdd') end"
                elif column == '"settlement data 8"':
                    column = f"case when operational.isnumeric({column}) then to_date({column},'yymmdd') end"
                elif column == '"business date"':
                    column = f"case when operational.isnumeric({column}) then to_date({column},'yymmdd') end"
                elif column in ('"electronic commerce indicator 1"', '"electronic commerce indicator 2"', '"electronic commerce indicator 3"'):
                    column = f"case when {column} = '' then Null else {column} end"
                else:
                    column_str = column.replace('"','')
                    if df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_type'].values[0] in ['numeric','integer','bigint','timestamp','date']:
                        column = f"case when operational.isnumeric({column}) then {column}::{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_type'].values[0]} end"
                    else:
                        column = f"{column}::{df_adapter[(df_adapter['type_record'] == value) & (df_adapter['column_name'] == column_str)]['column_type'].values[0]}"
                select_query += f' {column},'
            
            left_query = f"""
            left join operational.m_currency c on (c.currency_numeric_code = case when operational.isnumeric(a."currency code transaction") then a."currency code transaction"::integer end) 
            left join operational.m_currency d on (d.currency_numeric_code = case when operational.isnumeric(a."currency code reconciliation") then a."currency code reconciliation"::integer end)
            left join operational.m_currency e on (e.currency_numeric_code = case when operational.isnumeric(a."currency code cardholder billing") then a."currency code cardholder billing"::integer end)  
            """
            str_column_dh = str_column_dh[:-1]
            insert_query = f'insert into {table_scheme}.{table_name}_{customer_code.lower()}_{type_file.lower()}_{string_date} ({str_column_dh}) '
            select_query = select_query[:-1] + f' from {table_scheme}.{table_name_stg}_{customer_code}_{type_file} a '
            query = insert_query + select_query + left_query
            if has_app_processing_date:
                query = f"{query} where app_processing_date = '{app_processing_date}'"
            partition = f"{table_name}_{customer_code}_{type_file}"
            result_message = self.ps.insert(query,[(table_name)])
            log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            str(result_message),
            self.module
            )
            table_count = self.ps.table_count(
                table_scheme,
                f'{table_name}_{customer_code}_{type_file}_{string_date}',
                f'where app_hash_file in ({hash_files_in})'
            )
            result_message = f'{table_count} {result_message} {table_name}'
            log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            result_message,
            self.module
            )
        return 'finished'

    def mastercard_load_exclusion_transaction(self, string_date: str = None,type_file:str=None, hash_file:str=None)->str:
        """Load of mastercard excluded transaction
        Args:
            string_date (str): date for range condition  
            type_file (str): Type of file
            type_file (str): hash code of file

        Returns:
            str: Message
        
        """
        module = 'ADAPTER'
        adapter_table_scheme = 'control'
        adapter_table_name = 't_mastercard_adapter'
        adapter_table = adapter_table_scheme+'.'+adapter_table_name
        customer_code = self.customer_code
        number_file = '1'
        ps_block = con.connect_to_postgreSQL(bool_query=True)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "mastercard file",
        "INFO",
        f"loading exclusion transactions type file : {type_file}",
        self.module
        ) 

        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')
        list_type_record = self.ps.select(adapter_table,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)",'distinct type_record')
        df_adapter = pd.DataFrame(self.ps.select(adapter_table,f"where to_date('{string_date}','yyyymmdd') between start_date and coalesce(end_date,current_date)"))
        for value in df_adapter['type_record'].drop_duplicates().values:
            table_scheme = 'operational'
            table_name = 'dh_mastercard_exclusion_messages'

            select_query = f"select app_id,app_customer_code,app_type_file,app_hash_file,app_processing_date,source_message_number_id,source_file_id from operational.dh_mastercard_data_element_{customer_code}_{type_file}_{string_date}  where app_message_type = '1644' and function_code =691"
            
            table_tmp = f'temporal.tmp_exclusion_transactions_{customer_code}_{type_file}_{string_date}'
            partition = f"{table_name}_{customer_code}_{type_file}"
            self.ps.drop_table(table_tmp)
            ps_block.drop_table(table_tmp)
            self.ps.create_table_from_select(select_query,table_tmp)
            result_message = str(self.ps.insert_from_table("temporal", table_tmp, table_scheme, table_name))

            log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            result_message,
            self.module
            )
        if self.debug == 'False':
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "MASTERCARD",
                self.log_name,
                "ADAPTER OF MASTERCARD FILE",
                "INFO",
                bulk_delete,
                self.module
            )
        return 'finished'
    
    def config_additional_table(self, string_start_date : str = None, string_end_date : str = None, config_table_name : str = None) ->str:
        """Configuration of the additional tables
        Args:
            string_start_date (str): start date for range condition  
            string_end_date (str): ending date for range condition
            config_table_name (str): name of table to config

        Returns:
            str: Message
        
        """
        adapter_table_scheme = 'operational'
        adapter_table_name = 'additional_tables_structure'
        adapter_table = adapter_table_scheme+'.'+adapter_table_name
        column_base = [{'column_name': 'app_id','length': '10', 'column_type': 'numeric'},{'column_name': 'app_customer_code','length': '50', 'column_type': 'text'},{'column_name': 'app_type_file','length': '50', 'column_type': 'text'},{'column_name': 'app_hash_file','length': '300', 'column_type': 'text'},{'column_name': 'app_processing_date','length': '30', 'column_type': 'date'}]
        list_except_table_type = ['exchange_rate']
        if string_start_date == None:
            string_start_date = datetime.now().strftime('%Y%m%d')
        if string_end_date == None:
            string_end_date = string_start_date
        customer_table = 'control.t_customer'
        list_table = self.ps.select(adapter_table,' order by table_name,column_order')
        df_table = pd.DataFrame(list_table)
        list_customer = self.ps.select(customer_table,"where status = 'ACTIVE'",'name, code')
        table_scheme = 'operational'
        for table_name in df_table['table_name'].drop_duplicates().values:
            columns = []
            table_type = df_table[df_table['table_name'] == table_name].reset_index(drop=True)['table_type'][0]
            table_frequency = df_table[df_table['table_name'] == table_name].reset_index(drop=True)['table_load_frequency'][0]
            customer_code = self.customer_code
            if config_table_name != None and config_table_name != table_name:
                continue
            columns.extend(column_base)
            for column_name in df_table[df_table['table_name'] == table_name]['column_name']:
                df_row = df_table[(df_table['table_name'] == table_name) & (df_table['column_name']== column_name)].reset_index(drop=True)
                columns.append({'column_name':df_row['column_name'][0],'length':df_row['column_length'][0],'column_type':df_row['column_type'][0],'column_decimal':df_row['column_decimal'][0]})
            if not self.ps.table_exists(table_scheme+'.'+table_name):
                if table_type in list_except_table_type:
                    result_message = self.ps.create_table(columns,table_scheme+'.'+table_name,True,'app_processing_date','range')
                else:
                    result_message = self.ps.create_table(columns,table_scheme+'.'+table_name,True,'app_customer_code','list')
                log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA AND MASTERCARD FILE",
                "INFO",
                result_message,
                self.module
                )
            df_x = pd.DataFrame(self.ps.get_structure_table_from_db(table_scheme,table_name))
            df_y = pd.DataFrame(columns)
            if len(df_x) != len(df_y):
                list_table_difference = []
                for index, value in enumerate(df_y['column_name'].values):
                    if not value in (df_x['column_name'].values):
                        list_table_difference.append({'column_name': df_y['column_name'][index], 'length' : df_y['length'][index], 'column_type': df_y['column_type'][index]})
                result_message = self.ps.add_column(list_table_difference,table_scheme,table_name)
                log.logs().exist_file(
                "OPERATIONAL",
                self.customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA AND MASTERCARD FILE",
                "INFO",
                result_message,
                self.module
                )

            if table_type == 'transaction':
                partition_name = table_name+'_'+customer_code
                message_partition = self.ps.create_table_partition_list(table_scheme,table_name,table_scheme,partition_name,f"'{customer_code}'",'app_type_file','list')

                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )

                message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_in',f"'IN'",'app_processing_date','range')

                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )
                message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_out',f"'OUT'",'app_processing_date','range')

                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )
     
                message_partition = self.ps.create_table_partition_range(table_scheme,partition_name+'_in',table_scheme,partition_name+'_in',string_start_date,string_end_date,table_frequency)
                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )


                message_partition = self.ps.create_table_partition_range(table_scheme,partition_name+'_out',table_scheme,partition_name+'_out',string_start_date,string_end_date,table_frequency)

                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )


            elif table_type in list_except_table_type:
                partition_name = table_name
                message_partition = self.ps.create_table_partition_range(table_scheme,partition_name,table_scheme,partition_name,string_start_date,string_end_date,table_frequency)
                log.logs().exist_file(
                "OPERATIONAL",
                table_type,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )
                message_create_table_index = self.ps.create_table_index(table_scheme,table_name,table_name+'_date_idx','app_processing_date')
                log.logs().exist_file(
                    "OPERATIONAL",
                    self.customer_code,
                    "VISA AND MASTERCARD",
                    self.log_name,
                    "ADAPTER OF VISA and MASTERCARD FILE",
                    "INFO",
                    str(message_create_table_index),
                    self.module
                )
            else:
                partition_name = table_name+'_'+customer_code
                message_partition = self.ps.create_table_partition_list(table_scheme,table_name,table_scheme,partition_name,f"'{customer_code}'",'app_type_file','list')

                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )
                message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_'+table_type,f"'{table_type}'",'app_processing_date','range')

                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )      
                message_partition = self.ps.create_table_partition_range(table_scheme,partition_name+'_'+table_type,table_scheme,partition_name+'_'+table_type,string_start_date,string_end_date,table_frequency)
                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )
        return 'finished'

    def config_additional_table_others(self, string_start_date : str = None, string_end_date : str = None,log_name: str = None, client:str = None) ->str:
        """Configuration of the other additional tables
        
        Args:
            string_start_date (str): start date for range condition  
            string_end_date (str): ending date for range condition

        Returns:
            str: Message
        
        """
        adapter_table_scheme = 'operational'
        adapter_table_name = 'additional_tables_structure'
        adapter_table = adapter_table_scheme+'.'+adapter_table_name
        column_base = [{'column_name': 'app_id','length': '10', 'column_type': 'numeric'},{'column_name': 'app_customer_code','length': '50', 'column_type': 'text'},{'column_name': 'app_type_file','length': '50', 'column_type': 'text'},{'column_name': 'app_hash_file','length': '300', 'column_type': 'text'},{'column_name': 'app_processing_date','length': '30', 'column_type': 'date'}]
        if string_start_date == None:
            string_start_date = datetime.now().strftime('%Y%m%d')
        if string_end_date == None:
            string_end_date = string_start_date
        customer_table = 'control.t_customer'
        list_table = self.ps.select(adapter_table,' order by table_name,column_order')
        df_table = pd.DataFrame(list_table)
        list_customer = self.ps.select(customer_table,"where status = 'ACTIVE'",'name, code')
        table_scheme = 'operational'
        for table_name in df_table['table_name'].drop_duplicates().values:
            columns = []
            columns.extend(column_base)
            for column_name in df_table[df_table['table_name'] == table_name]['column_name']:
                df_row = df_table[(df_table['table_name'] == table_name) & (df_table['column_name']== column_name)].reset_index(drop=True)
                columns.append({'column_name':df_row['column_name'][0],'length':str(int(df_row['column_length'][0])),'column_type':df_row['column_type'][0],'column_decimal':str(int(df_row['column_decimal'][0]))})
            if not self.ps.table_exists(table_scheme+'.'+table_name):

                result_message = self.ps.create_table(columns,table_scheme+'.'+table_name,True,'app_customer_code','list')
                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                result_message,
                self.module
                )    

            df_x = pd.DataFrame(self.ps.get_structure_table_from_db(table_scheme,table_name))
            df_y = pd.DataFrame(columns)
            if len(df_x) != len(df_y):
                list_table_difference = []
                for index, value in enumerate(df_y['column_name'].values):
                    if not value in (df_x['column_name'].values):
                        list_table_difference.append({'column_name': df_y['column_name'][index], 'length' : df_y['length'][index], 'column_type': df_y['column_type'][index]})
                message_column = self.ps.add_column(list_table_difference,table_scheme,table_name)

                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_column,
                self.module
                )    


            for customer in list_customer:
                customer_code = customer['code']
                partition_name = table_name+'_'+customer_code
                message_partition = self.ps.create_table_partition_list(table_scheme,table_name,table_scheme,partition_name,f"'{customer_code}'",'app_type_file','list')
                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )    

                message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_in',f"'IN'",'app_processing_date','range')
                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )    
                message_partition = self.ps.create_table_partition_range(table_scheme,partition_name+'_in',table_scheme,partition_name+'_in',string_start_date,string_end_date)
                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )  
                message_partition = self.ps.create_table_partition_list(table_scheme,partition_name,table_scheme,partition_name+'_out',f"'OUT'",'app_processing_date','range')
                
                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )      
       
                message_partition  = self.ps.create_table_partition_range(table_scheme,partition_name+'_out',table_scheme,partition_name+'_out',string_start_date,string_end_date)
        
                log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA AND MASTERCARD",
                self.log_name,
                "ADAPTER OF VISA and MASTERCARD FILE",
                "INFO",
                message_partition,
                self.module
                )  
        
        
        return 'finished'

    def visa_load_calculated_field_dh(self, string_date: str = None, type_file: str = None, hash_files: list = [])->str:
        """Loading calculated fields from visa
        
        Args:
            string_date (str): date for range condition  
            type_file (str): type of file
            hash_files (str): list hash code of file

        Returns:
            str: Message

        """
        customer_code = self.customer_code
        table_scheme = 'operational'
        table_name = f'dh_visa_transaction_calculated_field_{customer_code.lower()}_{type_file.lower()}_{string_date}'
        adapter_table = table_scheme+'.'+table_name
        number_file = '1'
        module = 'ADAPTER'
        ps_block = con.connect_to_postgreSQL(bool_query=True)

        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')

        log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            f"calculating field {table_scheme}.{table_name} with type_file: {type_file}  and date {string_date}",
            self.module
        )
        hash_files_in = "'{}'".format("','".join(hash_files))
        query_tmp = f"""
        with ardef_pre_1 as (
            select distinct 
            app_date_valid,
            rpad(left(trim(low_key_for_range),9),16,'0')::BIGINT low_key_for_range,
            rpad(left(trim(table_key),9),16,'9')::BIGINT high_key_for_range,
            ardef_country, a.account_funding_source,a.product_id,a.delete_indicator
            ,country issuer_country
            ,technology_indicator
            ,fast_funds
            ,travel_indicator
            ,b2b_program_id
            ,nnss_indicator
            ,product_subtype
            from operational.dh_visa_ardef a
            where app_date_valid<=to_date('{string_date}','yyyymmdd')
            --group by 1,2,3,4,5,6,7
        ), ardef_pre_r as (
            select a.* --app_date_valid,low_key_for_range,high_key_for_range,ardef_country,account_funding_source,product_id,
            ,row_number()over(partition by low_key_for_range order by app_date_valid desc,delete_indicator,high_key_for_range desc) rn
            from ardef_pre_1 a
        )
        select * from ardef_pre_r where rn=1
        """
        
        table_scheme_tmp = 'temporal'
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_ardef_unique'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'

        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            message_drop,
            self.module
        )
        ps_block.drop_table(table_tmp)

        create_message = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        create_message,
        self.module
        )  
        
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_calculated_field_pre'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        query_tmp = f"""
        create temporary table tmp_pre_dh_visa_transaction_{customer_code}_{type_file}_{string_date} as
            select * from operational.dh_visa_transaction_{customer_code}_{type_file}_{string_date}
            where app_hash_file in ({hash_files_in});

        create index tmp_pre_dh_visa_transaction_account_number_idx
            on tmp_pre_dh_visa_transaction_{customer_code}_{type_file}_{string_date} (account_number);

        create table {table_tmp} as
        select
        t.app_id,t.app_customer_code,t.app_type_file,t.app_hash_file,t.app_processing_date,
        case
            when right(t.authorization_code, 1) = 'x' then 'INVALID'
            when right(t.authorization_code, 5) in ('     ','00000','0000 ','0000n','0000p','0000y') then 'INVALID'
            else 'VALID'
        end authorization_code_valid,
        case
            when t.app_type_file = 'IN' then 'issuing'
            when t.app_type_file = 'OUT' then 'acquiring'
            else t.app_type_file
        end business_mode,
        case
                when a.ardef_country = trim(t.merchant_country_code) then
                    case
                        when t.app_type_file = 'IN' then
                            case
                                when string_to_array(left(t.account_reference_number_acquiring_identifier::text,6),',') <@ string_to_array(c.acquiring_bins,',') or upper(t.collection_only_flag) = 'C' then 'on-us'
                                else 'off-us'
                            end
                        when t.app_type_file = 'OUT' then
                            case
                                when (string_to_array(left(t.account_number::text,6),',') <@ string_to_array(c.issuing_bins_6_digits,',') or string_to_array(left(t.account_number::text,8),',') <@ string_to_array(c.issuing_bins_8_digits,',')) or upper(t.collection_only_flag) = 'C' then 'on-us'
                                else 'off-us'
                            end
                    end
                when a.ardef_country <> trim(t.merchant_country_code) and cra.visa_region_code = crt.visa_region_code then 'intraregional'
                when a.ardef_country <> trim(t.merchant_country_code) and cra.visa_region_code <> crt.visa_region_code then 'interregional'
        end jurisdiction, crt.country_code jurisdiction_country,r.region_code jurisdiction_region
        ,a.ardef_country
        ,t.merchant_country_code
        ,central_processing_date - t.purchase_date timeless
        ,a.account_funding_source funding_source, a.product_id
        ,row_number()over(partition by t.app_id,t.app_hash_file order by a.app_date_valid desc,a.high_key_for_range desc) n
        ,issuer_country
        ,technology_indicator
        ,fast_funds
        ,travel_indicator
        ,b2b_program_id
        ,nnss_indicator
        ,product_subtype
        ,t.settlement_flag
        from tmp_pre_dh_visa_transaction_{customer_code}_{type_file}_{string_date} t
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_visa_ardef_unique a
        on (t.account_number between low_key_for_range and high_key_for_range)
        left join operational.m_country crt
        on (crt.country_code = trim(t.merchant_country_code))
        left join operational.m_country cra
        on (cra.country_code = a.ardef_country)
        left join operational.m_region r
        on (r.region_code = crt.visa_region_code)
        left join control.t_customer c
        on (upper(c.code) = upper('{customer_code}'));
        """
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            message_drop,
            self.module
        )
        ps_block.drop_table(table_tmp)

        message_create = self.ps.execute_block(query_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )  
        query_tmp = f"""
        select 
        t.app_id,t.app_customer_code,t.app_type_file,t.app_hash_file,t.app_processing_date
        ,t.destination_amount
        ,t.source_amount
        ,t.destination_currency_code
        ,t.source_currency_code
        ,t.transaction_code
        ,t.merchant_category_code
        ,t.special_condition_indicator_merchant_transaction_indicator
        ,t.transaction_code_qualifier_0
        ,t.usage_code
        ,case
            when t.source_currency_code = cast(mcu.currency_numeric_code as varchar(3))
            then 1
            else ex.exchange_value
        end exchange_value_settlement
        ,case
            when t.source_currency_code = cast(mcu2.currency_numeric_code as varchar(3))
            then 1
            else ex2.exchange_value
        end exchange_value_local
        ,cus.local_currency_code
        from operational.dh_visa_transaction_{customer_code}_{type_file}_{string_date} t
        left join control.t_customer cus on cus.code = t.app_customer_code
        left join operational.m_currency mcu on cus.settlement_currency_code = mcu.currency_alphabetic_code
        left join operational.m_currency mcu2 on cus.local_currency_code = mcu2.currency_alphabetic_code
        left join operational.dh_exchange_rate ex
        on (ex.app_processing_date = t.app_processing_date
        and t.source_currency_code = ex.currency_from_code
        and ex.currency_to = cus.settlement_currency_code 
        and ex.brand='VISA')
        left join operational.dh_exchange_rate ex2
        on (ex2.app_processing_date = t.app_processing_date
        and t.source_currency_code = ex2.currency_from_code
        and ex2.currency_to = cus.local_currency_code
        and ex2.brand='VISA')
        
        where t.app_hash_file in ({hash_files_in})
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_sfc_pre'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            message_drop,
            self.module
        )
        ps_block.drop_table(table_tmp)  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )

        query_tmp = f"""
        select
        t.app_id,t.app_customer_code,t.app_type_file,t.app_hash_file,t.app_processing_date,
        case t.app_type_file
            when 'IN' then
                case 
                    when transaction_code in ('05','15','25','35') then 
                        case         
                            when special_condition_indicator_merchant_transaction_indicator in ('7','8') then 3
                            when special_condition_indicator_merchant_transaction_indicator not in ('7','8') then 1
                            else 255
                        end
                    when transaction_code in ('07','17','27','37') then 
                        case 
                            when merchant_category_code in (6010) then 21 --MANUAL CASH
                            when merchant_category_code in (6011) then 22 -- ATM
                            else 255
                        end 
                    when transaction_code in ('06','16','26','36') then 
                        case
                            when usage_code = 1 and transaction_code_qualifier_0 = 2 then 25
                            when usage_code = 1 then 19
                            when usage_code = 1 and special_condition_indicator_merchant_transaction_indicator in ('7','8') then 20
                            else 255
                        end
                    else 255
                end
            when 'OUT' then
                case 
                    when transaction_code in ('05','15','25','35') then 
                        case         
                            when merchant_category_code not in (4829, 6051, 7995) then 1 -- purchase 1
                            when merchant_category_code in (4829, 6051, 7995) then 3 -- quasi cash
                            else 255
                        end
                    when transaction_code in ('07','17','27','37') then 
                        case 
                            when merchant_category_code  in (6010) then 21 --MANUAL CASH
                            when merchant_category_code  in (6011) then 22 -- ATM
                            else 255
                        end 
                    when transaction_code in ('06','16','26','36') then 
                        case        
                            when usage_code = 1 and transaction_code_qualifier_0 = 2 then 25
                            when usage_code = 1 then 19
                            when usage_code = 1 and special_condition_indicator_merchant_transaction_indicator in ('7','8') then 20
                            else 255
                        end
                    else 255
                end
        end business_transaction_type
        ,case
            when transaction_code in('05','06','07') then 
                case
                    when usage_code = 1 then 11
                    when usage_code = 2 then 23
                    when usage_code = 9 then 6
                    else 255
                end
            when transaction_code in ('15','16','17','35','36','37') then
                case
                    when usage_code = 1 then 1
                    when usage_code = 9 then 4
                    else 255
                end
            when transaction_code in ('25','26','27') then
                case
                    when usage_code = 1 then 11 -- reversal must be 1
                    when usage_code = 9 then 6  --reversal must be 1
                    when usage_code = 2 then 25
                    else 255
                end 
            else 255
        end business_transaction_cycle
        ,case when transaction_code::integer >= 20 and transaction_code::integer<30 and usage_code in (1,9) then 1 else 0
        end reversal_indicator
        ,case
            when dhvtc.jurisdiction in ('on-us','off-us') and dhvtc.settlement_flag != 0
            then
                case 
                    when t.exchange_value_local is not null
                    then t.source_amount*t.exchange_value_local
                end
            else
                case 
                    when t.exchange_value_settlement is not null
                    then t.source_amount*t.exchange_value_settlement
                end
        end::numeric settlement_report_amount
        ,case
            when dhvtc.jurisdiction in ('on-us','off-us') and dhvtc.settlement_flag != 0 
            then cus.local_currency_code
            else cus.settlement_currency_code 
        end settlement_report_currency_code
        from {table_tmp} t
        inner join temporal.{customer_code}_{type_file}_{string_date}_visa_calculated_field_pre dhvtc 
        on (dhvtc.app_id = t.app_id and dhvtc.app_hash_file = t.app_hash_file)
        left join operational.m_currency cur on cur.currency_alphabetic_code = t.local_currency_code
        left join "control".t_customer cus on cus.code = t.app_customer_code
        where dhvtc.n = 1
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_sfc_field'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            message_drop,
            self.module
        )
        ps_block.drop_table(table_tmp)  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )  
        query_tmp = f"""
        select
        t.app_id,t.app_customer_code,t.app_type_file,t.app_hash_file,t.app_processing_date,
        tp.authorization_code_valid,tp.business_mode,lower(tp.jurisdiction) jurisdiction,tp.jurisdiction_country,tp.jurisdiction_region,tp.timeless,tp.funding_source,tp.product_id
        ,tp.ardef_country
        ,tp1.reversal_indicator
        ,tp1.business_transaction_type
        ,tp1.business_transaction_cycle
        ,case lower(tp.jurisdiction) when 'intraregional' then tp.jurisdiction_region::text
            when 'interregional' then '9'
            else tp.jurisdiction_country::text
        end jurisdiction_assigned
        ,tp.issuer_country
        ,tp.technology_indicator
        ,tp.fast_funds
        ,tp.travel_indicator
        ,tp.b2b_program_id
        ,tp.nnss_indicator
        ,tp.product_subtype
        ,tp1.settlement_report_amount
        ,tp1.settlement_report_currency_code
        from operational.dh_visa_transaction_{customer_code}_{type_file}_{string_date} t
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_visa_calculated_field_pre tp
        on (t.app_id = tp.app_id and t.app_hash_file = tp.app_hash_file)
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_visa_sfc_field tp1
        on (t.app_id = tp1.app_id and t.app_hash_file = tp1.app_hash_file)
        where tp.n = 1
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_calculated_field'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            message_drop,
            self.module
        )
        ps_block.drop_table(table_tmp)  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )  

        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        self.ps.insert_from_table(table_scheme_tmp, table_name_tmp,table_scheme, table_name),
        self.module
        )
        print(f"Debug is {self.debug}")
        if self.debug == 'False':
            
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA",
                self.log_name,
                "ADAPTER OF VISA FILE",
                "INFO",
                bulk_delete,
                self.module
            )
        return 'finished'

    def mastercard_load_calculated_field_dh(self, string_date: str = None, type_file: str = None, hash_files: list = [])->str:
        """Loading calculated fields from mastercard
        
        Args:
            string_date (str): date for range condition  
            type_file (str): type of file
            hash_files (str): list hash code of file

        Returns:
            str: Message
        
        """
        customer_code = self.customer_code
        table_scheme = 'operational'
        table_name = f'dh_mastercard_calculated_field_{customer_code.lower()}_{type_file.lower()}_{string_date}'
        adapter_table = table_scheme+'.'+table_name
        number_file = '1'
        ps_block = con.connect_to_postgreSQL(bool_query=True)
        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')
        hash_files_in = "'{}'".format("','".join(hash_files))
        query_tmp = f"""
        with iar_pre_1 as (
            select 
            to_date(a.effective_timestamp,'yyddd24H') effective_date, to_date(to_char(a.app_date_valid::timestamp,'yyyy-mm-dd'),'yyyy-mm-dd') app_date_valid, 
            to_date(to_char(a.app_date_end::timestamp,'yyyy-mm-dd'),'yyyy-mm-dd') app_date_end, 
            left(a.low_range::varchar,18)::numeric low_key_for_range,
            left(a.high_range::varchar,18)::numeric high_key_for_range,
            a.card_country_alpha iar_country, 
            a.card_program_priority::numeric card_program_priority,
            a.card_program_identifier
            ,b.gcms_product_id gcms_product_identifier
            ,b.product_category funding_source
            from operational.dh_mastercard_iar a 
            left join operational.m_mastercard_brand_product b
            on (b.licensed_product_id=a.licensed_product_id and b.active_inactive_code='A')
            where to_date(to_char(a.app_date_valid::timestamp,'yyyy-mm-dd'),'yyyy-mm-dd')<=to_date('{string_date}','yyyymmdd')
            and a.active_inactive_code='A'
        ), bin_pre_2 as (
            select a.* 
            ,row_number()over(partition by low_key_for_range order by app_date_valid desc,card_program_priority) rn
            from iar_pre_1 a
        )
        select 
        app_date_valid,low_key_for_range::numeric low_key_for_range,high_key_for_range::numeric high_key_for_range,iar_country,gcms_product_identifier,funding_source,card_program_identifier
        from bin_pre_2 a
        where rn=1
        """
        table_scheme_tmp = 'temporal'
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_mastercard_iar_unique'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'

        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_create,
        self.module
        )  

        
        query_tmp = f"""
        select
        a.app_id,a.app_customer_code,a.app_type_file,a.app_hash_file,a.app_processing_date,
        a."settlement_indicator_1" settlement_indicator,
        left(rpad(pan::varchar,18,'0'),18)::numeric num_card, 
        substring(acquirer_reference_data, 2, 6) acquirer_bin,
        a."date_and_time_local_transaction" purchase_date,
        a.card_acceptor_country_code card_purchase_country, 
        b."mastercard_region_code" card_purchase_region
        from operational.dh_mastercard_data_element_{customer_code}_{type_file}_{string_date} a
        left join operational.m_country b
        on b."country_code_alternative" = a.card_acceptor_country_code
        where a.app_message_type in ('1240','1442') and a.app_hash_file in ({hash_files_in})
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_pre'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'

        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_index = self.ps.create_table_index(table_scheme_tmp,table_name_tmp,f'{customer_code}_{type_file}_{string_date}_mccf_pre_idx','num_card')
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_index,
        self.module
        )  


        query_tmp = f"""
        select 
        a.app_id,a.app_customer_code,a.app_type_file,a.app_hash_file,a.app_processing_date,
        case 
            when a.app_type_file = 'IN'  then 'issuing'
            when a.app_type_file = 'OUT' then 'acquiring'
            else a.app_type_file 
        end business_mode
        ,case
            when a.card_purchase_country = b.iar_country then
                case
                    when a.app_type_file = 'IN' then 
                        case 
                            when string_to_array(a.acquirer_bin,',') <@ string_to_array(c.acquiring_bins,',') or upper(a.settlement_indicator) = 'C' then 'on-us'
                            else 'off-us'
                        end
                    when a.app_type_file = 'OUT' then 
                        case
                            when (string_to_array(left(a.num_card::text,6),',') <@ string_to_array(c.issuing_bins_6_digits,',') or string_to_array(left(a.num_card::text,8),',') <@ string_to_array(c.issuing_bins_8_digits,',')) or upper(a.settlement_indicator) = 'C' then 'on-us'
                            else 'off-us'
                        end     
                end    
            when a.card_purchase_country <> b.iar_country and bc.mastercard_region_code = ac.mastercard_region_code then 'intraregional'
            when a.card_purchase_country <> b.iar_country and bc.mastercard_region_code <> ac.mastercard_region_code then 'interregional'
            end jurisdiction, bc.country_code jurisdiction_country,r.region_code::text jurisdiction_region
        ,b.*
        from {table_tmp} a
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_mastercard_iar_unique b
        on a.num_card >= b.low_key_for_range and a.num_card <= b.high_key_for_range
        left join operational.m_country ac
        on (ac.country_code_alternative = a.card_purchase_country)
        left join operational.m_country bc
        on (bc.country_code_alternative = b.iar_country)
        left join operational.m_region r
        on (r.region_code = bc.mastercard_region_code)
        left join control.t_customer c
        on (upper(c.code) = upper('{customer_code}'))
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_pre_1'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'

        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_create,
        self.module
        )

        query_tmp = f"""
        select a.*,
            row_number() over (partition by a.app_id, a.app_hash_file order by a.app_date_valid desc, a.high_key_for_range desc) n
        from {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_pre_1 a
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_pre_2'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'

        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_create,
        self.module
        )

        query_tmp= f"""
        select t.app_id
            , t.app_hash_file
            , t.app_type_file
            , t.amount_reconciliation
            , t.amount_transaction
            , t.currency_code_transaction
            , t.currency_code_reconciliation
            , t.app_message_type
            , case
                when t.currency_code_transaction = mcu.currency_numeric_code
                    then 1
                else ex.exchange_value
            end                           exchange_value_settlement
            , case
                when t.currency_code_transaction = mcu2.currency_numeric_code
                    then 1
                else ex2.exchange_value
            end                           exchange_value_local
            , cus.local_currency_code
            , mcu2.currency_numeric_code local_currency_code_numeric
            , cus.settlement_currency_code
        from operational.dh_mastercard_data_element_{customer_code}_{type_file}_{string_date} t
                left join control.t_customer cus on cus.code = t.app_customer_code
                left join operational.m_currency mcu on cus.settlement_currency_code = mcu.currency_alphabetic_code
                left join operational.m_currency mcu2 on cus.local_currency_code = mcu2.currency_alphabetic_code
                left join operational.dh_exchange_rate_{string_date} ex
                        on t.app_processing_date = ex.app_processing_date
                            and ex.brand = 'MasterCard'
                            and t.currency_code_transaction = ex.currency_from_code::integer
                            and ex.currency_to = cus.settlement_currency_code
                left join operational.dh_exchange_rate_{string_date} ex2
                        on t.app_processing_date = ex2.app_processing_date
                            and ex2.brand = 'MasterCard'
                            and t.currency_code_transaction = ex2.currency_from_code::integer
                            and ex2.currency_to = cus.local_currency_code
        where t.app_hash_file in ({hash_files_in})
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_ex_rate'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'

        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        ) 

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_create,
        self.module
        )  
        
        query_tmp= f"""
        select app_hash_file,file_id
        from operational.dh_mastercard_data_element_{customer_code}_{type_file}_{string_date}
        where app_message_type = '1644' and function_code = 697 and app_hash_file in ({hash_files_in})
        group by 1,2
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_fi'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        ) 

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_create,
        self.module
        )

        query_tmp = f"""
        select 
        de.app_id,de.app_hash_file
        ,case 
            when de.app_type_file = 'IN' then f_currency_reconcilation.currency_alphabetic_code::text
            else
                case 
                    when jurisdiction in ('on-us','off-us') and de.currency_code_transaction = de.local_currency_code_numeric
                    then de.local_currency_code::text
                    else de.settlement_currency_code::text
                end    
        end as settlement_report_currency_code,
        case 
            when de.app_type_file = 'IN' then  amount_reconciliation 
            else 
                case
                    when jurisdiction in ('on-us','off-us') and de.currency_code_transaction = de.local_currency_code_numeric
                    then
                        case
                            when de.exchange_value_local is not null then  de.amount_transaction *  de.exchange_value_local
                        end
                    else
                        case
                            when de.exchange_value_settlement is not null then  de.amount_transaction *  de.exchange_value_settlement
                        end
                end
        end::numeric settlement_report_amount
        ,fi.file_id mc_file_id
        from {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_ex_rate de 
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_fi fi on (fi.app_hash_file = de.app_hash_file)
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_pre_2 cf on cf.app_id = de.app_id and cf.app_hash_file = de.app_hash_file
        left join operational.m_currency f_currency on f_currency.currency_numeric_code = currency_code_transaction 
        left join operational.m_currency f_currency_reconcilation on f_currency_reconcilation.currency_numeric_code = currency_code_reconciliation
        where  (de.app_message_type = '1240' or de.app_message_type='1422')
        and cf.n=1
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_amount'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        ) 

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_create,
        self.module
        )  

        query_tmp = f"""
        select
        t.app_id,t.app_customer_code,t.app_type_file,t.app_hash_file,t.app_processing_date,
        tp.business_mode,tp.jurisdiction,tp.jurisdiction_country,tp.jurisdiction_region,tp.funding_source,
        tp.gcms_product_identifier,
        tp.card_program_identifier,
        case tp.jurisdiction when 'intraregional' then tp.jurisdiction_region
            when 'interregional' then '9'
            else tp.jurisdiction_country
        end jurisdiction_assigned,
        tp1.settlement_report_currency_code,tp1.settlement_report_amount,tp1.mc_file_id
        ,tp.iar_country
        from operational.dh_mastercard_data_element_{customer_code}_{type_file}_{string_date} t
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_pre_2 tp
        on (t.app_id = tp.app_id and t.app_hash_file = tp.app_hash_file)
        left join {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_amount tp1
        on (tp1.app_id = t.app_id and tp1.app_hash_file = t.app_hash_file)
        where tp.n=1
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_mastercard_calculated_field'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'

        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_create,
        self.module
        )  

        message_insert = self.ps.insert_from_table(table_scheme_tmp, table_name_tmp,table_scheme, table_name)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "ADAPTER OF MASTERCARD FILE",
        "INFO",
        message_insert,
        self.module
        )

        query_complete = f"""
        insert into {table_scheme}.{table_name}
        select a.app_id, a.app_customer_code, a.app_type_file, a.app_hash_file, a.app_processing_date,
        case
            when a.app_type_file = 'IN'  then 'issuing'
            when a.app_type_file = 'OUT' then 'acquiring'
            else a.app_type_file
        end business_mode,
        null jurisdiction,
        null jurisdiction_country,
        null jurisdiction_region,
        null funding_source,
        null gcms_product_identifier,
        null card_program_identifier,
        null jurisdiction_assigned,
        null settlement_report_currency_code,
        null settlement_report_amount,
        null mc_file_id,
        null iar_country
        from {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_mastercard_calculated_field_pre a
        left join {table_scheme}.{table_name} b
        on a.app_id = b.app_id and a.app_hash_file = b.app_hash_file
        where b.app_id is null and b.app_hash_file is null
        """
        self.ps.insert(query_complete, [(table_name)])

        if self.debug == 'False':
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "MASTERCARD",
                self.log_name,
                "ADAPTER OF MASTERCARD FILE",
                "INFO",
                bulk_delete,
                self.module
            )

        return 'finished'
    
    def visa_load_sms_calculated_field_dh(self, string_date: str = None,type_file: str = None, hash_files: list = [])->str:
        """Loading calculated fields from visa sms
        
        Args:
            string_date (str): date for range condition  
            type_file (str): type of file
            hash_files (str): list hash code of file

        Returns:
            str: Message
        
        """
        customer_code = self.customer_code
        table_scheme = 'operational'
        table_name = f'dh_visa_transaction_sms_calculated_field_{customer_code.lower()}_{type_file.lower()}_{string_date}'
        adapter_table = table_scheme+'.'+table_name
        number_file = '1'
        module = 'ADAPTER'
        ps_block = con.connect_to_postgreSQL(bool_query=True)
        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        "loading sms calculated field dh"
        + table_scheme
        + "."
        + table_name,
        self.module
        )   

        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')
        hash_files_in = "'{}'".format("','".join(hash_files))
        query_tmp = f"""
        with ardef_pre_1 as (
            select distinct 
            app_date_valid,
            rpad(left(trim(low_key_for_range),9),19,'0')::numeric low_key_for_range,
            rpad(left(trim(table_key),9),19,'9')::numeric high_key_for_range,
            ardef_country, a.account_funding_source,a.product_id,a.delete_indicator
            ,country issuer_country
            ,technology_indicator
            ,fast_funds
            ,travel_indicator
            ,b2b_program_id
            ,nnss_indicator
            ,product_subtype
            from operational.dh_visa_ardef a
            where app_date_valid<=to_date('{string_date}','yyyymmdd')
            --group by 1,2,3,4,5,6,7
        ), ardef_pre_r as (
            select a.* --app_date_valid,low_key_for_range,high_key_for_range,ardef_country,account_funding_source,product_id,
            ,row_number()over(partition by low_key_for_range order by app_date_valid desc,delete_indicator,high_key_for_range desc) rn
            from ardef_pre_1 a
        )
        select * from ardef_pre_r where rn=1
        """
        
        table_scheme_tmp = 'temporal'
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_sms_ardef_unique'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'

        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )  

        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_sms_calculated_field_pre'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        query_tmp = f"""
        create temporary table tmp_pre_dh_sms_transaction_{customer_code}_{type_file}_{string_date} as
            select * from operational.dh_visa_transaction_sms_{customer_code}_{type_file}_{string_date}
            where app_hash_file in ({hash_files_in});

        create index tmp_pre_dh_sms_transaction_card_number_idx
            on tmp_pre_dh_sms_transaction_{customer_code}_{type_file}_{string_date} (card_number);

        create table {table_tmp} as
        select
        t.app_id,t.app_customer_code,t.app_type_file,t.app_hash_file,t.app_processing_date,t.request_message_type
        ,case
            when request_message_type in ('0200','0220') and response_code = '00' then 0
            when request_message_type in ('0400','0420') and response_code = '00' then 1
        end as reversal_indicator,
        case
            when request_message_type in ('0200','0220','0400','0420') and response_code = '00' then
                case
                    when left(processing_code,2) = '00' and pos_condition_code not in ('13','51') and merchants_type not in (4815,6010,6011) then 1    -- Purchase
                    when left(processing_code,2) = '01' and pos_condition_code not in ('13','51') and merchants_type     in (6010)           then 21   -- Manual Cash
                    when left(processing_code,2) = '01' and pos_condition_code not in ('13','51') and merchants_type     in (6011)           then 22   -- ATM Cash
                    when left(processing_code,2) = '10' and pos_condition_code not in ('13','51') and merchants_type not in (4815,6010,6011) then 30   -- Account Funding
                    when left(processing_code,2) = '11' and pos_condition_code not in ('13','51') and merchants_type not in (4815,6010,6011) then 3    -- Quasi-Cash
                    when left(processing_code,2) = '19' and pos_condition_code not in ('13','51') and merchants_type not in (4815,6010,6011) then 115  -- Fee Collection
                    when left(processing_code,2) = '20' and pos_condition_code not in ('13','51') and merchants_type not in (4815,6010,6011) then 19   -- Merchandise Credit
                    when left(processing_code,2) = '22' and pos_condition_code     in ('13')      and merchants_type not in (4815,6010,6011) then 20   -- Quasi-Cash Credit
                    when left(processing_code,2) = '26' and pos_condition_code not in ('13','51') and merchants_type not in (4815,6010,6011) then 25   -- Original Credit
                    when left(processing_code,2) = '29' and pos_condition_code not in ('13','51') and merchants_type not in (4815,6010,6011) then 200  -- Funds Disbursement
                    when left(processing_code,2) = '30' and pos_condition_code not in ('13','51') and merchants_type     in (6011)           then 247  -- ATM Balance Inquiry
                    when left(processing_code,2) = '40' and pos_condition_code not in ('13','51') and merchants_type     in (6011)           then 250  -- ATM Transfer
                    when left(processing_code,2) = '50' and pos_condition_code not in ('13','51') and merchants_type not in (4815,6010,6011) then 27   -- Payment Order
                end
            when request_message_type in ('0200','0220','0400','0420') and response_code <> '00' then
                case
                    when merchants_type <> 6011 then 236  -- POS Decline
                    when merchants_type =  6011 then 249  -- ATM Decline
                end
        end as business_transaction_type
        ,null::int as business_transaction_cycle
        ,case
            when right(t.authorization_id_resp_code, 1) = 'x' then 'INVALID'
            when right(t.authorization_id_resp_code, 5) in ('     ','00000','0000 ','0000n','0000p','0000y') then 'INVALID'
            else 'VALID'
        end authorization_code_valid,
        case upper(t.issuer_acquirer_indicator)
            when 'A' then 'acquiring'
            when 'I' then 'issuing'
        end business_mode,
        case
                when a.ardef_country = trim(t.card_acceptor_country) then
                    case upper(t.issuer_acquirer_indicator)
                        when 'A' then
                            case
                                when (string_to_array(left(t.card_number::text,6),',') <@ string_to_array(c.issuing_bins_6_digits,',') or string_to_array(left(t.card_number::text,8),',') <@ string_to_array(c.issuing_bins_8_digits,',')) then 'on-us'
                                else 'off-us'
                            end
                        when 'I' then
                            case
                                when string_to_array(left(t.acquiring_institution_id_1::text,6),',') <@ string_to_array(c.acquiring_bins,',') then 'on-us'
                                else 'off-us'
                            end
                    end
                when a.ardef_country <> trim(t.card_acceptor_country) and cra.visa_region_code = crt.visa_region_code then 'intraregional'
                when a.ardef_country <> trim(t.card_acceptor_country) and cra.visa_region_code <> crt.visa_region_code then 'interregional'
        end jurisdiction, crt.country_code jurisdiction_country,r.region_code jurisdiction_region
        ,a.ardef_country
        ,t.card_acceptor_country
        ,app_processing_date::date - t.local_transaction_date::date timeless
        ,a.account_funding_source funding_source, a.product_id
        ,row_number()over(partition by t.app_id,t.app_hash_file order by a.app_date_valid desc,a.high_key_for_range desc) n
        ,issuer_country
        ,technology_indicator
        ,fast_funds
        ,travel_indicator
        ,b2b_program_id
        ,nnss_indicator
        ,product_subtype
        from tmp_pre_dh_sms_transaction_{customer_code}_{type_file}_{string_date} t
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_visa_sms_ardef_unique a
        on (t.card_number between low_key_for_range and high_key_for_range)
        left join operational.m_country crt
        on (crt.country_code = trim(t.card_acceptor_country))
        left join operational.m_country cra
        on (cra.country_code = a.ardef_country)
        left join operational.m_region r
        on (r.region_code = crt.visa_region_code)
        left join control.t_customer c
        on (upper(c.code) = upper('{customer_code}'));
        """
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.execute_block(query_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )

        query_tmp = f"""
        select
        t.*,
        case
            when b.transaction_type_id = 'PUR' and t.reversal_indicator = 0 then '05'
            when b.transaction_type_id = 'CRD' and t.reversal_indicator = 0 then '06'
            when b.transaction_type_id = 'CSH' and t.reversal_indicator = 0 then '07'
            when b.transaction_type_id = 'PUR' and t.reversal_indicator = 1 then '25'
            when b.transaction_type_id = 'CRD' and t.reversal_indicator = 1 then '26'
            when b.transaction_type_id = 'CSH' and t.reversal_indicator = 1 then '27'
        end transaction_code_sms
        from {table_tmp} t
        left join operational.m_visa_business_transaction_type b
            on t.business_transaction_type::varchar = b.business_transaction_type_id;
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_sms_calculated_field_pre_2'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create= self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA SMS file",
        "INFO",
        message_create,
        self.module
        )

        query_tmp = f"""
        select
        t.app_id,t.app_customer_code,t.app_type_file,t.app_hash_file,t.app_processing_date,
        tp.authorization_code_valid,tp.business_mode,lower(tp.jurisdiction) jurisdiction,tp.jurisdiction_country,tp.jurisdiction_region,tp.timeless,tp.funding_source,tp.product_id
        ,tp.ardef_country
        ,tp.reversal_indicator
        ,tp.business_transaction_type
        ,tp.business_transaction_cycle
        ,tp.transaction_code_sms
        ,case lower(tp.jurisdiction) when 'intraregional' then tp.jurisdiction_region::text
            when 'interregional' then '9'
            else tp.jurisdiction_country::text
        end jurisdiction_assigned
        ,tp.issuer_country
        ,tp.technology_indicator
        ,tp.fast_funds
        ,tp.travel_indicator
        ,tp.b2b_program_id
        ,tp.nnss_indicator
        ,tp.product_subtype
        ,mcu.currency_alphabetic_code settlement_report_currency_code
        ,t.settlement_amount settlement_report_amount
        from operational.dh_visa_transaction_sms_{customer_code}_{type_file}_{string_date} t
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{string_date}_visa_sms_calculated_field_pre_2 tp
        on (t.app_id = tp.app_id and t.app_hash_file = tp.app_hash_file)
        left join operational.m_currency mcu on cast(t.settlement_currency_code_sms as varchar(3)) = cast(mcu.currency_numeric_code as varchar(3))
        where tp.n = 1
        """
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_sms_calculated_field'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )  

        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        self.ps.insert_from_table(table_scheme_tmp, table_name_tmp,table_scheme, table_name),
        self.module
        )
        if self.debug == 'False':
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA",
                self.log_name,
                "ADAPTER OF VISA FILE",
                "INFO",
                bulk_delete,
                self.module
            )
        return 'finished'

    def visa_load_vss_110_calculated_field_dh(self, string_date: str = None,type_file:str = None, hash_files: list = [])->str:
        """Loading calculated fields from visa vss 110
        
        Args:
            string_date (str): date for range condition  
            type_file (str): type of file
            hash_files (str): list hash code of file

        Returns:
            str: Message
        
        """
        customer_code = self.customer_code
        table_scheme = 'operational'
        table_name = f'dh_visa_transaction_vss_calculated_field_{customer_code.lower()}_{type_file.lower()}_{string_date}'
        adapter_table = table_scheme+'.'+table_name
        number_file = '1'
        module = 'ADAPTER'
        ps_block = con.connect_to_postgreSQL(bool_query=True)
        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')


        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        "loading vss 110 calculated field dh into "
        + table_scheme
        + "."
        + table_name,
        self.module
        )
        hash_files_in = "'{}'".format("','".join(hash_files))
        query_tmp = f"""
        with rollup_group as (
        select rollup_to_sre_identifier_110 rollup_group
        from operational.dh_visa_transaction_vss_110_{customer_code}_{type_file}_{string_date}
        where rollup_to_sre_identifier_110 <> reporting_for_sre_identifier_110
        group by 1
        ), rollup_1 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_110 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_110, reporting_for_sre_identifier_110
            from operational.dh_visa_transaction_vss_110_{customer_code}_{type_file}_{string_date}
            where rollup_to_sre_identifier_110 <> reporting_for_sre_identifier_110 and app_hash_file in ({hash_files_in})
        ), rollup_2 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_110 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_110, reporting_for_sre_identifier_110
            from rollup_1
        ), rollup_3 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_110 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_110, reporting_for_sre_identifier_110
            from rollup_2
        )
        select  
        a.app_id,a.app_customer_code,a.app_type_file,a.app_hash_file,a.app_processing_date
        ,110 report_type
        ,case 
            when a.rollup_to_sre_identifier_110 = a.reporting_for_sre_identifier_110 then 10
            when b.reporting_indicator is not null then b.reporting_indicator
            when c.reporting_indicator is not null then c.reporting_indicator
            when d.reporting_indicator is not null then d.reporting_indicator
            else 0
        end aggregation_level
        from operational.dh_visa_transaction_vss_110_{customer_code}_{type_file}_{string_date} a
        left join rollup_1 b on (a.app_id = b.app_id and a.app_hash_file = b.app_hash_file)
        left join rollup_2 c on (a.app_id = c.app_id and a.app_hash_file = c.app_hash_file)
        left join rollup_3 d on (a.app_id = d.app_id and a.app_hash_file = d.app_hash_file)
        where a.app_hash_file in ({hash_files_in})
        """
        table_scheme_tmp = 'temporal'
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_vss_110_calculated_field'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_drop,
        self.module
        )  


        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )  

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        self.ps.insert_from_table(table_scheme_tmp, table_name_tmp,table_scheme, table_name),
        self.module
        )

        if self.debug == 'False':
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA",
                self.log_name,
                "ADAPTER OF VISA FILE",
                "INFO",
                bulk_delete,
                self.module
            )
        return 'finished'

    def visa_load_vss_120_calculated_field_dh(self, string_date: str = None,type_file:str = None, hash_files: list = [])->str:
        """Loading calculated fields from visa vss 120
        
        Args:
            string_date (str): date for range condition  
            type_file (str): type of file
            hash_files (str): list hash code of file

        Returns:
            str: Message
        
        """
        customer_code = self.customer_code
        table_scheme = 'operational'
        table_name = f'dh_visa_transaction_vss_calculated_field_{customer_code.lower()}_{type_file.lower()}_{string_date}'
        adapter_table = table_scheme+'.'+table_name
        number_file = '1'
        module = 'ADAPTER'
        ps_block = con.connect_to_postgreSQL(bool_query=True)
        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')

        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        "loading vss 120 calculated field dh into "
        + table_scheme
        + "."
        + table_name,
        self.module
        )

        hash_files_in = "'{}'".format("','".join(hash_files))
        query_tmp = f"""
        with rollup_group as (
        select rollup_to_sre_identifier_120 rollup_group
        from operational.dh_visa_transaction_vss_120_{customer_code}_{type_file}_{string_date}
        where rollup_to_sre_identifier_120 <> reporting_for_sre_identifier_120 and app_hash_file in ({hash_files_in})
        group by 1
        ), rollup_1 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_120 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_120, reporting_for_sre_identifier_120
            from operational.dh_visa_transaction_vss_120_{customer_code}_{type_file}_{string_date}
            where rollup_to_sre_identifier_120 <> reporting_for_sre_identifier_120 and app_hash_file in ({hash_files_in})
        ), rollup_2 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_120 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_120, reporting_for_sre_identifier_120
            from rollup_1
        ), rollup_3 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_120 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_120, reporting_for_sre_identifier_120
            from rollup_2
        )
        select  
        a.app_id,a.app_customer_code,a.app_type_file,a.app_hash_file,a.app_processing_date
        ,120 report_type
        ,case 
            when a.rollup_to_sre_identifier_120 = a.reporting_for_sre_identifier_120 then 10
            when b.reporting_indicator is not null then b.reporting_indicator
            when c.reporting_indicator is not null then c.reporting_indicator
            when d.reporting_indicator is not null then d.reporting_indicator
            else 0
        end aggregation_level
        from operational.dh_visa_transaction_vss_120_{customer_code}_{type_file}_{string_date} a
        left join rollup_1 b on (a.app_id = b.app_id and a.app_hash_file = b.app_hash_file)
        left join rollup_2 c on (a.app_id = c.app_id and a.app_hash_file = c.app_hash_file)
        left join rollup_3 d on (a.app_id = d.app_id and a.app_hash_file = d.app_hash_file)
        where a.app_hash_file in ({hash_files_in})
        """
        table_scheme_tmp = 'temporal'
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_vss_120_calculated_field'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_drop,
        self.module
        )  

        
        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )  

        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        self.ps.insert_from_table(table_scheme_tmp, table_name_tmp,table_scheme, table_name),
        self.module
        )

        if self.debug == 'False':
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA",
                self.log_name,
                "ADAPTER OF VISA FILE",
                "INFO",
                bulk_delete,
                self.module
            )
        return 'finished'

    def visa_load_vss_130_calculated_field_dh(self, string_date: str = None,type_file:str = None, hash_files: list = [])->str:
        """Loading calculated fields from visa vss 130
        
        Args:
            string_date (str): date for range condition  
            type_file (str): type of file
            hash_files (str): list hash code of file

        Returns:
            str: Message
        
        """
        customer_code = self.customer_code
        table_scheme = 'operational'
        table_name = f'dh_visa_transaction_vss_calculated_field_{customer_code.lower()}_{type_file.lower()}_{string_date}'
        adapter_table = table_scheme+'.'+table_name
        number_file = '1'
        module = 'ADAPTER'
        ps_block = con.connect_to_postgreSQL(bool_query=True)
        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')

        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        "loading vss 130 calculated field dh into "
        + table_scheme
        + "."
        + table_name,
        self.module
        )
        hash_files_in = "'{}'".format("','".join(hash_files))
        query_tmp = f"""
        with rollup_group as (
        select rollup_to_sre_identifier_130 rollup_group
        from operational.dh_visa_transaction_vss_130_{customer_code}_{type_file}_{string_date}
        where rollup_to_sre_identifier_130 <> reporting_for_sre_identifier_130 and app_hash_file in ({hash_files_in})
        group by 1
        ), rollup_1 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_130 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_130, reporting_for_sre_identifier_130
            from operational.dh_visa_transaction_vss_130_{customer_code}_{type_file}_{string_date}
            where rollup_to_sre_identifier_130 <> reporting_for_sre_identifier_130 and app_hash_file in ({hash_files_in})
        ), rollup_2 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_130 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_130, reporting_for_sre_identifier_130
            from rollup_1
        ), rollup_3 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_130 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_130, reporting_for_sre_identifier_130
            from rollup_2
        )
        select  
        a.app_id,a.app_customer_code,a.app_type_file,a.app_hash_file,a.app_processing_date
        ,130 report_type
        ,case 
            when a.rollup_to_sre_identifier_130 = a.reporting_for_sre_identifier_130 then 10
            when b.reporting_indicator is not null then b.reporting_indicator
            when c.reporting_indicator is not null then c.reporting_indicator
            when d.reporting_indicator is not null then d.reporting_indicator
            else 0
        end aggregation_level
        from operational.dh_visa_transaction_vss_130_{customer_code}_{type_file}_{string_date} a
        left join rollup_1 b on (a.app_id = b.app_id and a.app_hash_file = b.app_hash_file)
        left join rollup_2 c on (a.app_id = c.app_id and a.app_hash_file = c.app_hash_file)
        left join rollup_3 d on (a.app_id = d.app_id and a.app_hash_file = d.app_hash_file)
        where a.app_hash_file in ({hash_files_in})
        """
        table_scheme_tmp = 'temporal'
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_vss_130_calculated_field'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )  
        
        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        self.ps.insert_from_table(table_scheme_tmp, table_name_tmp,table_scheme, table_name),
        self.module
        )

        if self.debug == 'False':
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA",
                self.log_name,
                "ADAPTER OF VISA FILE",
                "INFO",
                bulk_delete,
                self.module
            )
        return 'finished'
    
    def visa_load_vss_140_calculated_field_dh(self, string_date: str = None,type_file:str=None, hash_files: list = [])->str:
        """Loading calculated fields from visa vss 140

        Args:
            string_date (str): date for range condition  
            type_file (str): type of file
            hash_files (str): list hash code of file

        Returns:
            str: Message
        
        """
        customer_code = self.customer_code
        table_scheme = 'operational'
        table_name = f'dh_visa_transaction_vss_calculated_field_{customer_code.lower()}_{type_file.lower()}_{string_date}'
        adapter_table = table_scheme+'.'+table_name
        number_file = '1'
        module = 'ADAPTER'
        ps_block = con.connect_to_postgreSQL(bool_query=True)
        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')


        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        "loading vss 140 calculated field dh into "
        + table_scheme
        + "."
        + table_name,
        self.module
        )

        hash_files_in = "'{}'".format("','".join(hash_files))
        query_tmp = f"""
        with rollup_group as (
        select rollup_to_sre_identifier_140 rollup_group
        from operational.dh_visa_transaction_vss_140_{customer_code}_{type_file}_{string_date}
        where rollup_to_sre_identifier_140 <> reporting_for_sre_identifier_140 and app_hash_file in ({hash_files_in})
        group by 1
        ), rollup_1 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_140 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_140, reporting_for_sre_identifier_140
            from operational.dh_visa_transaction_vss_140_{customer_code}_{type_file}_{string_date}
            where rollup_to_sre_identifier_140 <> reporting_for_sre_identifier_140
        ), rollup_2 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_140 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_140, reporting_for_sre_identifier_140
            from rollup_1
        ), rollup_3 as (
            select app_id,app_hash_file,case when reporting_for_sre_identifier_140 in (select rollup_group from rollup_group) then 1 else 0 end reporting_indicator, rollup_to_sre_identifier_140, reporting_for_sre_identifier_140
            from rollup_2
        )
        select  
        a.app_id,a.app_customer_code,a.app_type_file,a.app_hash_file,a.app_processing_date
        ,140 report_type
        ,case 
            when a.rollup_to_sre_identifier_140 = a.reporting_for_sre_identifier_140 then 10
            when b.reporting_indicator is not null then b.reporting_indicator
            when c.reporting_indicator is not null then c.reporting_indicator
            when d.reporting_indicator is not null then d.reporting_indicator
            else 0
        end aggregation_level
        from operational.dh_visa_transaction_vss_140_{customer_code}_{type_file}_{string_date} a
        left join rollup_1 b on (a.app_id = b.app_id and a.app_hash_file = b.app_hash_file)
        left join rollup_2 c on (a.app_id = c.app_id and a.app_hash_file = c.app_hash_file)
        left join rollup_3 d on (a.app_id = d.app_id and a.app_hash_file = d.app_hash_file)
        where a.app_hash_file in ({hash_files_in})
        """
        table_scheme_tmp = 'temporal'
        table_name_tmp = f'{customer_code}_{type_file}_{string_date}_visa_vss_140_calculated_field'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)

        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_drop,
        self.module
        ) 

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        )  

        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        self.ps.insert_from_table(table_scheme_tmp, table_name_tmp,table_scheme, table_name),
        self.module
        )
        if self.debug == 'False':
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA",
                self.log_name,
                "ADAPTER OF VISA FILE",
                "INFO",
                bulk_delete,
                self.module
            )
        return 'finished'
    
    def load_mastercard_interchange(self, string_date: str = None,type_file: str = None, hash_file:str =None, number_file:str=None)->str:
        """Mastercard exchange charge per adapter
        
        Args:
            string_date (str): date for range condition  
            type_file (str): type of file
            hash_file (str): hash code of file
            number_file (str): number of file in sequence

        Returns:
            str: Message
        
        """
        customer_code = self.customer_code
        table_scheme = 'operational'
        table_name = f'dh_mastercard_interchange_{customer_code.lower()}_{type_file.lower()}_{string_date}'
        adapter_table = table_scheme+'.'+table_name
        module = 'ADAPTER'
        str_currency = ''
        str_condition_currency = ''
        ps_block = con.connect_to_postgreSQL(bool_query=True)
        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')

        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "MASTERCARD",
        self.log_name,
        "INTERCHANGE OF MASTERCARD FILE",
        "INFO",
        "loading mastercard interchange"
        + table_scheme
        + "."
        + table_name,
        self.module
        )   
        table_rules = self.ps.select('operational.m_interchange_rules_mc','where amount_transaction_currency is not null group by 1','amount_transaction_currency')
        if len(table_rules)>0:
            for rules in table_rules:
                str_currency += f",coalesce((t.amount_transaction*case when cu.currency_alphabetic_code = '{rules['amount_transaction_currency']}' then 1 else ex_{rules['amount_transaction_currency']}.exchange_value end)::text,'BLANK') amount_transaction_{rules['amount_transaction_currency']} "
                str_condition_currency += f"left join operational.dh_exchange_rate ex_{rules['amount_transaction_currency']} on (ex_{rules['amount_transaction_currency']}.app_processing_date=to_date(to_char(t.date_and_time_local_transaction,'yyyymmdd'),'yyyymmdd') and ex_{rules['amount_transaction_currency']}.currency_from = cu.currency_alphabetic_code and ex_{rules['amount_transaction_currency']}.currency_to='{rules['amount_transaction_currency']}' and ex_{rules['amount_transaction_currency']}.brand='VISA') "

        query_tmp = f"""
        SELECT 
        coalesce(trim(left(t.pan::text, 8)),'BLANK') issuer_bin_8
        ,substring(acquirer_reference_data, 2, 6) acquirer_bin
        ,coalesce(trim(t.electronic_commerce_indicator_3),'BLANK') electronic_commerce_indicator_3
        ,t.app_id,t.app_customer_code,t.app_type_file,t.app_hash_file,t.app_processing_date
        ,coalesce(tcf.jurisdiction_assigned::text,'BLANK') jurisdiction
        ,coalesce(trim(t.business_activity_4)::text,'BLANK') ird
        ,coalesce(trim(left(t.processing_code,2)),'BLANK') processing_code
        ,coalesce(t.amount_transaction::text,'BLANK') amount_transaction
        ,coalesce(cu.currency_alphabetic_code::text,'BLANK') amount_transaction_currency
        {str_currency}
        ,coalesce(t.card_acceptor_business_code_mcc::text,'BLANK') card_acceptor_business_code
        ,coalesce(trim(tcf.gcms_product_identifier)::text,'BLANK') gcms_product_identifier
        ,coalesce(trim(tcf.funding_source),'BLANK') funding_source
        from operational.dh_mastercard_data_element_{customer_code}_{type_file}_{string_date} t
        inner join operational.dh_mastercard_calculated_field_{customer_code}_{type_file}_{string_date} tcf
        on (tcf.app_id = t.app_id and tcf.app_hash_file = t.app_hash_file)
        left join operational.m_currency cu
        on (cu.currency_numeric_code = t.currency_code_transaction::int)
        {str_condition_currency}
        where t.app_hash_file='{hash_file}'
        """
        table_scheme_tmp = 'temporal'
        table_name_tmp = f'{customer_code}_{type_file}_{number_file}_mastercard_interchange_eval'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "INTERCHANGE OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "INTERCHANGE OF MASTERCARD FILE",
        "INFO",
        message_create,
        self.module
        )  

        table_name_tmp = self.mastercard_interchange_rule_assign(table_scheme_tmp,table_name_tmp,string_date)
        if table_name_tmp == '-1':
            if self.debug == 'False':
                bulk_delete = ps_block.query_block()
                log.logs().exist_file(
                    "OPERATIONAL",
                    customer_code,
                    "MASTERCARD",
                    self.log_name,
                    "INTERCHANGE OF MASTERCARD FILE",
                    "INFO",
                    bulk_delete,
                    self.module
                )
            return 'finished'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        ps_block.drop_table(table_tmp)
        query_tmp = f"""
        with tmp_pre as
        (
        select to_date(to_char(a.date_and_time_local_transaction,'yyyymmdd'),'yyyymmdd') date_and_time_local_transaction,a.currency_code_transaction::numeric currency_code_transaction
        from operational.dh_mastercard_data_element_{customer_code}_{type_file}_{string_date} a
        group by 1,2
        )
        select *
        from operational.dh_exchange_rate er
        where (er.app_processing_date,er.currency_from_code::numeric) in (select date_and_time_local_transaction,currency_code_transaction from tmp_pre)
        and er.brand='MasterCard'
        """
        table_name_tmp = f'{customer_code}_{type_file}_{number_file}_mastercard_ex_rate'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "INTERCHANGE OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "INTERCHANGE OF MASTERCARD FILE",
        "INFO",
        message_create,
        self.module
        )

        query_tmp = f"""
        select distinct
        a.app_id,a.app_customer_code,a.app_type_file,a.app_hash_file,a.app_processing_date
        ,a.amount_transaction
        ,a.currency_code_transaction currency_transaction
        ,c.rate_currency
        ,c.rate_variable::numeric
        ,c.rate_fixed::numeric
        ,c.rate_min::numeric
        ,c.rate_cap::numeric
        ,case
            when c.rate_variable is null then rate_fixed::numeric
            when not operational.isnumeric(c.rate_variable) then null
            else
                case
                    when (rate_variable::numeric * (a.amount_transaction*case when c.rate_currency is null then 1 when c.rate_currency = cur.currency_alphabetic_code then 1 else er.exchange_value end)) + coalesce(c.rate_fixed,0) <= rate_min then rate_min::numeric
                    when (rate_variable::numeric * (a.amount_transaction*case when c.rate_currency is null then 1 when c.rate_currency = cur.currency_alphabetic_code then 1 else er.exchange_value end)) + coalesce(c.rate_fixed,0) >= rate_cap then rate_cap::numeric
                    else (rate_variable::numeric * (a.amount_transaction*case when c.rate_currency is null then 1 when c.rate_currency = cur.currency_alphabetic_code then 1 else er.exchange_value end)) + coalesce(c.rate_fixed,0)
                end
        end calculated_value,
        e.intelica_id::numeric,
        e.region_country_code,
        e.ird
        from operational.dh_mastercard_data_element_{customer_code}_{type_file}_{string_date} a
        inner join temporal.{customer_code}_{type_file}_{number_file}_mastercard_interchange_eval_assign e
        on (a.app_id = e.app_id::numeric and a.app_hash_file = e.app_hash_file)
        left join operational.m_interchange_rules_mc c
        on (c.intelica_id = e.intelica_id and c.region_country_code = e.region_country_code and c.ird = e.ird 
        and to_date('{string_date}','yyyymmdd') between c.valid_from::date and coalesce(c.valid_until::date,current_date))
        left join operational.m_currency cur on cur.currency_numeric_code = a.currency_code_transaction::numeric
        left join {table_tmp} er
        on (er.date=to_date(to_char(a.date_and_time_local_transaction,'yyyymmdd'),'yyyymmdd') and er.currency_from_code::numeric=a.currency_code_transaction::numeric and er.currency_to=c.rate_currency and lower(er.brand)='mastercard')
        """
        table_name_tmp = f'{customer_code}_{type_file}_{number_file}_mastercard_interchange'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "INTERCHANGE OF MASTERCARD FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "INTERCHANGE OF MASTERCARD FILE",
        "INFO",
        message_create,
        self.module
        )  

        message_insert = self.ps.insert_from_table(table_scheme_tmp, table_name_tmp,table_scheme, table_name)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "MASTERCARD",
        self.log_name,
        "INTERCHANGE OF MASTERCARD FILE",
        "INFO",
        message_insert,
        self.module
        )  

        if self.debug == 'False':
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "MASTERCARD",
                self.log_name,
                "INTERCHANGE OF MASTERCARD FILE",
                "INFO",
                bulk_delete,
                self.module
            )

        return 'finished'
    
    def load_visa_interchange(self, string_date: str = None,type_file: str = None, hash_file:str =None, number_file:str=None)->str:
        """Visa exchange charge per adapter
        
        Args:
            string_date (str): date for range condition  
            type_file (str): type of file
            hash_file (str): hash code of file
            number_file (str): number of file in sequence

        Returns:
            str: Message
        
        """
        customer_code = self.customer_code
        table_scheme = 'operational'
        table_name = f'dh_visa_interchange_{customer_code.lower()}_{type_file.lower()}_{string_date}'
        adapter_table = table_scheme+'.'+table_name
        str_currency = ''
        str_condition_currency = ''
        module = 'ADAPTER'
        ps_block = con.connect_to_postgreSQL(bool_query=True)
        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA FILE",
        "INFO",
        "loading visa interchange "
        + table_scheme
        + "."
        + table_name,
        self.module
        )

        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')

        table_rules = self.ps.select('operational.m_interchange_rules_visa','where transaction_amount_currency is not null group by 1','transaction_amount_currency')
        if len(table_rules)>0:
            for rules in table_rules:
                str_currency += f",coalesce((t.source_amount*case when cu.currency_alphabetic_code = '{rules['transaction_amount_currency']}' then 1 else ex_{rules['transaction_amount_currency']}.exchange_value end)::text,'BLANK') transaction_amount_{rules['transaction_amount_currency']} "
                str_condition_currency += f"left join operational.dh_exchange_rate ex_{rules['transaction_amount_currency']} on (ex_{rules['transaction_amount_currency']}.app_processing_date=t.purchase_date and ex_{rules['transaction_amount_currency']}.currency_from = cu.currency_alphabetic_code and ex_{rules['transaction_amount_currency']}.currency_to='{rules['transaction_amount_currency']}' and ex_{rules['transaction_amount_currency']}.brand='VISA') "

        query_tmp = f"""
        SELECT 
        t.app_id,t.app_customer_code,t.app_type_file,t.app_hash_file,t.app_processing_date
        ,coalesce(upper(tcf.jurisdiction_assigned)::text,'BLANK') jurisdiction
        ,coalesce(upper(tcf.business_mode)::text,'BLANK') business_mode,
        coalesce(tcf.ardef_country::text,'BLANK') issuer_country,
        coalesce(c.visa_region_code::text,'BLANK') issuer_region
        ,coalesce(tcf.technology_indicator::text,'BLANK')technology_indicator
        ,coalesce(tcf.product_id::text,'BLANK')product_id
        ,coalesce(tcf.fast_funds::text,'BLANK')fast_funds
        ,coalesce(tcf.travel_indicator::text,'BLANK')travel_indicator
        ,coalesce(tcf.b2b_program_id::text,'BLANK')b2b_program_id
        ,coalesce(tcf.funding_source::text,'BLANK') account_funding_source
        ,coalesce(tcf.nnss_indicator::text,'BLANK')nnss_indicator
        ,coalesce(tcf.product_subtype::text,'BLANK')product_subtype
        ,coalesce(right(lpad(t.transaction_code::text,2,'0'),2),'BLANK') transaction_code
        ,coalesce(t.transaction_code_qualifier_0::text,'BLANK') transaction_code_qualifier
        ,coalesce(t.account_number::text,'BLANK') payment_credential_combination
        ,coalesce(t.account_reference_number_acquiring_identifier::text,'BLANK') acquiring_identifier
        ,coalesce(t.source_amount::text,'BLANK') transaction_amount
        {str_currency}
        ,coalesce(cu.currency_alphabetic_code::text,'BLANK') transaction_amount_currency
        ,coalesce(t.merchant_country_code::text,'BLANK') acquirer_country
        ,coalesce(c2.visa_region_code::text,'BLANK') acquirer_region
        ,coalesce(t.merchant_country_code::text,'BLANK')merchant_country_code
        ,coalesce(c2.visa_region_code::text,'BLANK') merchant_country_region
        ,coalesce(t.merchant_category_code::text,'BLANK')merchant_category_code
        ,coalesce(t.requested_payment_service::text,'BLANK')requested_payment_service
        ,coalesce(t.usage_code::text,'BLANK')usage_code
        ,coalesce(t.authorization_characteristics_indicator::text,'BLANK')authorization_characteristics_indicator
        ,coalesce(tcf.authorization_code_valid::text,'BLANK') authorization_code
        ,coalesce(t.pos_terminal_capacity::text,'BLANK') pos_terminal_capability
        ,coalesce(t.cardholder_id_method::text,'BLANK')cardholder_id_method
        ,coalesce(t.pos_entry_mode::text,'BLANK')pos_entry_mode
        ,coalesce(tcf.timeless::text,'BLANK') timeliness
        ,coalesce(t.reimbursement_attribute::text,'BLANK')reimbursement_attribute
        ,coalesce(t.special_condition_indicator_merchant_transaction_indicator::text,'BLANK') special_condition_indicator
        ,coalesce(t.fee_program_indicator::text,'BLANK')fee_program_indicator
        ,'BLANK' processing_code
        ,coalesce(t.motoec_indicator::text,'BLANK') moto_eci_indicator
        ,coalesce(t.acceptance_terminal_indicator::text,'BLANK')acceptance_terminal_indicator
        ,coalesce(t.prepaid_card_indicator::text,'BLANK')prepaid_card_indicator
        ,coalesce(t.pos_environment::text,'BLANK') pos_environment_code
        ,coalesce(case
            when business_format_code_sp = 'SP' then business_format_code_sp
            when business_format_code_sd = 'SD' then business_format_code_sd
            when business_format_code_pd = 'PD' then business_format_code_pd
            when business_format_code_ft = 'FT' then business_format_code_ft
            when business_format_code_fl = 'FL' then business_format_code_fl
            when business_format_code_df = 'DF' then business_format_code_df
            when business_format_code_cr = 'CR' then business_format_code_cr
        end::text,'BLANK') business_format_code
        ,coalesce(t.business_application_id_cr::text,'BLANK') business_application_id
        ,coalesce(t.type_of_purchase_fl::text,'BLANK') type_purchase
        ,coalesce(case 
            when business_format_code_sd = 'SD' then network_identification_code_sd
            when business_format_code_df = 'DF' then network_identification_code_df
            when business_format_code_sp = 'SP' then network_identification_code_sp
        end::text,'BLANK') network_identification_code
        ,coalesce(case 
            when business_format_code_sd = 'SD' then message_reason_code_sd
            when business_format_code_df = 'DF' then message_reason_code_df
            when business_format_code_sp = 'SP' then message_reason_code_sp
        end::text,'BLANK') message_reason_code
        ,coalesce(case 
            when business_format_code_sd = 'SD' then surcharge_amount_sd
            when business_format_code_sp = 'SP' then surcharge_amount_sp
        end::text,'BLANK') surcharge_amount
        ,coalesce(t.authorized_amount::text,'BLANK')authorized_amount
        ,coalesce(t.authorization_response_code::text,'BLANK')authorization_response_code
        ,coalesce(t.validation_code::text,'BLANK')validation_code
        ,coalesce(t.merchant_verification_value::text,'BLANK')merchant_verification_value
        ,coalesce(t.dcc_indicator::text,'BLANK') dynamic_currency_conversion_indicator
        ,coalesce(substr(t.pan_token::text,2,1),'BLANK') authorization_characteristics_indicator_5
        ,coalesce(t.cvv_result_code::text,'BLANK') cvv2_result_code
        ,coalesce(t.national_tax_included::text,'BLANK') national_tax_indicator
        ,coalesce(t.merchant_vat_registration_number::text,'BLANK') merchant_vat
        ,coalesce(t.summary_commodity_code::text,'BLANK') summary_commodity
        ,coalesce(t.message_identifier::text,'BLANK')message_identifier
        ,'BLANK' processing_code_transaction_type
        ,'BLANK' point_of_service_condition_code
        ,coalesce(left(t.account_Number::text,8),'BLANK') issuer_bin_8
        ,coalesce(t.account_reference_number_acquiring_identifier::text,'BLANK') acquirer_bin
        ,coalesce(t.acquirer_business_id::text,'BLANK') acquirer_business_id
        from operational.dh_visa_transaction_{customer_code}_{type_file}_{string_date} t
        inner join operational.dh_visa_transaction_calculated_field_{customer_code}_{type_file}_{string_date} tcf
        on (tcf.app_id = t.app_id and tcf.app_hash_file = t.app_hash_file)
        left join operational.m_country c
        on (c.country_code = tcf.ardef_country)
        left join operational.m_country c2
        on (c2.country_code = t.merchant_country_code)
        left join operational.m_currency cu
        on (cu.currency_numeric_code = t.source_currency_code::int)
        {str_condition_currency}
        where t.app_hash_file='{hash_file}'
        """
        table_scheme_tmp = 'temporal'
        table_name_tmp = f'{customer_code}_{type_file}_{number_file}_visa_interchange_eval'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA FILE",
        "INFO",
        message_create,
        self.module
        ) 

        table_name_tmp = self.visa_interchange_rule_assign(table_scheme_tmp,table_name_tmp,string_date)
        if table_name_tmp == '-1':
            if self.debug == 'False':
                bulk_delete = ps_block.query_block()
                log.logs().exist_file(
                    "OPERATIONAL",
                    customer_code,
                    "VISA",
                    self.log_name,
                    "INTERCHANGE OF VISA FILE",
                    "INFO",
                    bulk_delete,
                    self.module
                )
            return 'finished'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        ps_block.drop_table(table_tmp)
        query_tmp = f"""
        with tmp_pre as
        (
        select a.purchase_date,a.source_currency_code::numeric source_currency_code
        from operational.dh_visa_transaction_{customer_code}_{type_file}_{string_date} a
        group by 1,2
        )
        select *
        from operational.dh_exchange_rate er
        where (er.app_processing_date,er.currency_from_code::numeric) in (select purchase_date,source_currency_code from tmp_pre)
        and er.brand='VISA'
        """
        table_name_tmp = f'{customer_code}_{type_file}_{number_file}_visa_ex_pre'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA SMS FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA SMS FILE",
        "INFO",
        message_create,
        self.module
        )

        query_tmp = f"""
        select
        a.app_id,a.app_customer_code,a.app_type_file,a.app_hash_file,a.app_processing_date
        ,a.source_amount amount_transaction
        ,a.source_currency_code currency_transaction
        ,c.fee_currency
        ,c.fee_variable::numeric
        ,c.fee_fixed::numeric
        ,c.fee_min::numeric
        ,c.fee_cap::numeric
        ,case
            when c.fee_variable is null then fee_fixed::numeric
            when not operational.isnumeric(c.fee_variable::text) then null
            else
                case
                    when (fee_variable::numeric * (a.source_amount*case when c.fee_currency is null then 1 when c.fee_currency = cur.currency_alphabetic_code then 1 else er.exchange_value end)) + coalesce(c.fee_fixed,0) <= fee_min then fee_min::numeric
                    when (fee_variable::numeric * (a.source_amount*case when c.fee_currency is null then 1 when c.fee_currency = cur.currency_alphabetic_code then 1 else er.exchange_value end)) + coalesce(c.fee_fixed,0) >= fee_cap then fee_cap::numeric
                    else (fee_variable::numeric * (a.source_amount*case when c.fee_currency is null then 1 when c.fee_currency = cur.currency_alphabetic_code then 1 else er.exchange_value end)) + coalesce(c.fee_fixed,0)
                end
        end calculated_value,
        e.intelica_id::numeric
        ,e.region_country_code
        from operational.dh_visa_transaction_{customer_code}_{type_file}_{string_date} a
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{number_file}_visa_interchange_eval_assign e
        on (a.app_id = e.app_id::numeric and a.app_hash_file = e.app_hash_file)
        left join operational.m_interchange_rules_visa c
        on (c.intelica_id = e.intelica_id and c.region_country_code = e.region_country_code
        and to_date('{string_date}','yyyymmdd') between c.valid_from::date and coalesce(c.valid_until::date,current_date))
        left join operational.m_currency cur on cur.currency_numeric_code = a.source_currency_code::numeric
        left join {table_tmp} er
        on (er.date=a.purchase_date and er.currency_from_code::numeric=a.source_currency_code::numeric and er.currency_to=c.fee_currency and lower(er.brand)='visa')
        """
        table_name_tmp = f'{customer_code}_{type_file}_{number_file}_visa_interchange'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'

        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA FILE",
        "INFO",
        message_drop,
        self.module
        )   

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA FILE",
        "INFO",
        message_create,
        self.module
        ) 

        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA FILE",
        "INFO",
        self.ps.insert_from_table(table_scheme_tmp, table_name_tmp,table_scheme, table_name),
        self.module
        )
        
        if self.debug == 'False':
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA",
                self.log_name,
                "INTERCHANGE OF VISA FILE",
                "INFO",
                bulk_delete,
                self.module
            )
        return 'finished'
    
    def load_visa_sms_interchange(self, string_date: str = None,type_file: str = None, hash_file:str =None, number_file:str=None)->str:
        """Visa exchange charge per adapter
        
        Args:
            string_date (str): date for range condition  
            type_file (str): type of file
            hash_file (str): hash code of file
            number_file (str): number of file in sequence

        Returns:
            str: Message
        
        """
        customer_code = self.customer_code
        table_scheme = 'operational'
        table_name = f'dh_visa_sms_interchange_{customer_code.lower()}_{type_file.lower()}_{string_date}'
        adapter_table = table_scheme+'.'+table_name
        module = 'ADAPTER'
        str_currency = ''
        str_condition_currency = ''
        ps_block = con.connect_to_postgreSQL(bool_query=True)
        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA FILE",
        "INFO",
        "loading visa interchange "
        + table_scheme
        + "."
        + table_name,
        self.module
        )

        if string_date == None:
            string_date = datetime.now().strftime('%Y%m%d')
        table_rules = self.ps.select('operational.m_interchange_rules_visa','where transaction_amount_currency is not null group by 1','transaction_amount_currency')
        if len(table_rules)>0:
            for rules in table_rules:
                str_currency += f",coalesce((t.transaction_amount*case when cu.currency_alphabetic_code = '{rules['transaction_amount_currency']}' then 1 else ex_{rules['transaction_amount_currency']}.exchange_value end)::text,'BLANK') transaction_amount_{rules['transaction_amount_currency']} "
                str_condition_currency += f"left join operational.dh_exchange_rate ex_{rules['transaction_amount_currency']} on (ex_{rules['transaction_amount_currency']}.app_processing_date=t.local_transaction_date and ex_{rules['transaction_amount_currency']}.currency_from = cu.currency_alphabetic_code and ex_{rules['transaction_amount_currency']}.currency_to='{rules['transaction_amount_currency']}' and ex_{rules['transaction_amount_currency']}.brand='VISA') "

        query_tmp = f"""
        SELECT 
        t.app_id,t.app_customer_code,t.app_type_file,t.app_hash_file,t.app_processing_date
        ,coalesce(upper(tcf.jurisdiction_assigned)::text,'BLANK') jurisdiction
        ,coalesce(upper(tcf.business_mode)::text,'BLANK') business_mode,
        coalesce(tcf.ardef_country::text,'BLANK') issuer_country,
        coalesce(c.visa_region_code::text,'BLANK') issuer_region
        ,coalesce(tcf.technology_indicator::text,'BLANK')technology_indicator
        ,coalesce(tcf.product_id::text,'BLANK')product_id
        ,coalesce(tcf.fast_funds::text,'BLANK')fast_funds
        ,coalesce(tcf.travel_indicator::text,'BLANK')travel_indicator
        ,coalesce(tcf.b2b_program_id::text,'BLANK')b2b_program_id
        ,coalesce(tcf.funding_source::text,'BLANK') account_funding_source
        ,coalesce(tcf.nnss_indicator::text,'BLANK')nnss_indicator
        ,coalesce(tcf.product_subtype::text,'BLANK')product_subtype
        ,coalesce(tcf.transaction_code_sms,'BLANK') transaction_code
        --,coalesce(t.transaction_code_qualifier_0::text,'BLANK') transaction_code_qualifier
        --,coalesce(t.account_number::text,'BLANK') payment_credential_combination
        ,coalesce(t.transaction_amount::text,'BLANK') transaction_amount
        {str_currency}
        ,coalesce(cu.currency_alphabetic_code::text,'BLANK') transaction_amount_currency
        ,coalesce(t.card_acceptor_country::text,'BLANK') acquirer_country
        ,coalesce(c2.visa_region_code::text,'BLANK') acquirer_region
        ,coalesce(t.card_acceptor_country::text,'BLANK')merchant_country_code
        ,coalesce(c2.visa_region_code::text,'BLANK') merchant_country_region
        ,coalesce(t.merchants_type::text,'BLANK')merchant_category_code
        ,coalesce(t.requested_payment_service::text,'BLANK') requested_payment_service
        ,coalesce(t.usage_code_sms::text,'BLANK')usage_code
        ,coalesce(t.authorization_characteristics_indicator_sms::text,'BLANK')authorization_characteristics_indicator
        ,coalesce(tcf.authorization_code_valid::text,'BLANK') authorization_code
        ,coalesce(t.pos_terminal_entry_capability::text,'BLANK') pos_terminal_capability
        ,coalesce(t.customer_identification_method::text,'BLANK')cardholder_id_method
        ,coalesce(left(t.pos_entry_mode_sms::text,2),'BLANK')pos_entry_mode
        ,coalesce(tcf.timeless::text,'BLANK') timeliness
        ,coalesce(t.reimbursement_attribute_sms::text,'BLANK')reimbursement_attribute
        ,coalesce(t.chargeback_special_condition_merchant_indicator::text,'BLANK') special_condition_indicator
        ,coalesce(t.fee_program_indicator_sms::text,'BLANK')fee_program_indicator
        --,'BLANK' processing_code
        ,coalesce(t.mailtelephone_or_electronic_commerce_indicator::text,'BLANK') moto_eci_indicator
        ,coalesce(right(t.pos_terminal_type,1)::text,'BLANK')acceptance_terminal_indicator
        --,coalesce(t.prepaid_card_indicator::text,'BLANK')prepaid_card_indicator
        ,coalesce(t.recurring_payment_indicator_flag::text,'BLANK') pos_environment_code
        /*,coalesce(case
            when business_format_code_sp = 'SP' then business_format_code_sp
            when business_format_code_sd = 'SD' then business_format_code_sd
            when business_format_code_pd = 'PD' then business_format_code_pd
            when business_format_code_ft = 'FT' then business_format_code_ft
            when business_format_code_fl = 'FL' then business_format_code_fl
            when business_format_code_df = 'DF' then business_format_code_df
            when business_format_code_cr = 'CR' then business_format_code_cr
        end::text,'BLANK') business_format_code*/
        ,coalesce(t.business_application_identifier::text,'BLANK') business_application_id
        --,coalesce(t.type_of_purchase_fl::text,'BLANK') type_purchase
        ,coalesce(t.network_id::text,'BLANK') network_identification_code
        ,coalesce(t.message_reason_code_sms::text,'BLANK') message_reason_code
        ,coalesce(t.surcharge_amount_sms::text,'BLANK') surcharge_amount
        --,coalesce(t.authorized_amount::text,'BLANK')authorized_amount
        ,coalesce(t.response_code::text,'BLANK')authorization_response_code
        --,coalesce(t.validation_code::text,'BLANK')validation_code
        ,coalesce(t.mvv_code::text,'BLANK') merchant_verification_value
        ,coalesce(t.dcc_indicator_sms::text,'BLANK') dynamic_currency_conversion_indicator
        ,coalesce(t.cvv_result_code_sms::text,'BLANK') cvv2_result_code
        --,coalesce(t.national_tax_included::text,'BLANK') national_tax_indicator
        --,coalesce(t.merchant_vat_registration_number::text,'BLANK') merchant_vat
        --,coalesce(t.summary_commodity_code::text,'BLANK') summary_commodity
        --,coalesce(t.message_identifier::text,'BLANK')message_identifier
        ,coalesce(left(t.processing_code::text,2),'BLANK') processing_code_transaction_type
        ,coalesce(t.pos_condition_code::text,'BLANK') point_of_service_condition_code
        ,coalesce(left(t.card_number::text,8),'BLANK') issuer_bin_8
        ,coalesce(t.acquiring_institution_id_1::text,'BLANK') acquirer_bin
        ,coalesce(t.acquirer_business_id_sms::text,'BLANK') acquirer_business_id
        from operational.dh_visa_transaction_sms_{customer_code}_{type_file}_{string_date} t
        inner join operational.dh_visa_transaction_sms_calculated_field_{customer_code}_{type_file}_{string_date} tcf
        on (tcf.app_id = t.app_id and tcf.app_hash_file = t.app_hash_file)
        left join operational.m_country c
        on (c.country_code = tcf.ardef_country)
        left join operational.m_country c2
        on (c2.country_code = t.card_acceptor_country)
        left join operational.m_currency cu
        on (cu.currency_numeric_code = t.transaction_currency_code::int)
        {str_condition_currency}
        where t.app_hash_file='{hash_file}'
        """
        table_scheme_tmp = 'temporal'
        table_name_tmp = f'{customer_code}_{type_file}_{number_file}_visa_sms_interchange_eval'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA SMS FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "ADAPTER OF VISA SMS FILE",
        "INFO",
        message_create,
        self.module
        ) 

        table_name_tmp = self.visa_sms_interchange_rule_assign(table_scheme_tmp,table_name_tmp,string_date)
        if table_name_tmp == '-1':
            if self.debug == 'False':
                bulk_delete = ps_block.query_block()
                log.logs().exist_file(
                    "OPERATIONAL",
                    customer_code,
                    "VISA",
                    self.log_name,
                    "INTERCHANGE OF VISA SMS FILE",
                    "INFO",
                    bulk_delete,
                    self.module
                )
            return 'finished'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        ps_block.drop_table(table_tmp)
        query_tmp = f"""
        with tmp_pre as
        (
        select a.local_transaction_date,a.transaction_currency_code::numeric transaction_currency_code
        from operational.dh_visa_transaction_sms_{customer_code}_{type_file}_{string_date} a
        group by 1,2
        )
        select *
        from operational.dh_exchange_rate er
        where (er.app_processing_date,er.currency_from_code::numeric) in (select local_transaction_date,transaction_currency_code from tmp_pre)
        and er.brand='VISA'
        """
        table_name_tmp = f'{customer_code}_{type_file}_{number_file}_visa_sms_ex_pre'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA SMS FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA SMS FILE",
        "INFO",
        message_create,
        self.module
        )

        query_tmp = f"""
        select
        a.app_id,a.app_customer_code,a.app_type_file,a.app_hash_file,a.app_processing_date
        ,case when a.transaction_amount != 0 then a.transaction_amount else a.cryptogram_amount + a.surcharge_amount_sms end amount_transaction
        ,coalesce(a.transaction_currency_code, a.cryptogram_currency_code::text) currency_transaction
        ,c.fee_currency
        ,c.fee_variable::numeric
        ,c.fee_fixed::numeric
        ,c.fee_min::numeric
        ,c.fee_cap::numeric
        ,case
            when c.fee_variable is null then fee_fixed::numeric
            when not operational.isnumeric(c.fee_variable::text) then null
            when a.transaction_amount != 0 then
                case
                    when (fee_variable::numeric * (a.transaction_amount*case when c.fee_currency is null then 1 when c.fee_currency = cur.currency_alphabetic_code then 1 else er.exchange_value end)) + coalesce(c.fee_fixed,0) <= fee_min then fee_min::numeric
                    when (fee_variable::numeric * (a.transaction_amount*case when c.fee_currency is null then 1 when c.fee_currency = cur.currency_alphabetic_code then 1 else er.exchange_value end)) + coalesce(c.fee_fixed,0) >= fee_cap then fee_cap::numeric
                    else (fee_variable::numeric * (a.transaction_amount*case when c.fee_currency is null then 1 when c.fee_currency = cur.currency_alphabetic_code then 1 else er.exchange_value end)) + coalesce(c.fee_fixed,0)
                end
            else
                case
                    when (fee_variable::numeric * ((a.cryptogram_amount + a.surcharge_amount_sms)*case when c.fee_currency is null then 1 when c.fee_currency = cur2.currency_alphabetic_code then 1 else er2.exchange_value end)) + coalesce(c.fee_fixed,0) <= fee_min then fee_min::numeric
                    when (fee_variable::numeric * ((a.cryptogram_amount + a.surcharge_amount_sms)*case when c.fee_currency is null then 1 when c.fee_currency = cur2.currency_alphabetic_code then 1 else er2.exchange_value end)) + coalesce(c.fee_fixed,0) >= fee_cap then fee_cap::numeric
                    else (fee_variable::numeric * ((a.cryptogram_amount + a.surcharge_amount_sms)*case when c.fee_currency is null then 1 when c.fee_currency = cur2.currency_alphabetic_code then 1 else er2.exchange_value end)) + coalesce(c.fee_fixed,0)
                end
        end calculated_value,
        e.intelica_id::numeric
        ,e.region_country_code
        from operational.dh_visa_transaction_sms_{customer_code}_{type_file}_{string_date} a
        inner join {table_scheme_tmp}.{customer_code}_{type_file}_{number_file}_visa_sms_interchange_eval_assign e
        on (a.app_id = e.app_id::numeric and a.app_hash_file = e.app_hash_file)
        left join operational.m_interchange_rules_visa c
        on (c.intelica_id = e.intelica_id and c.region_country_code = e.region_country_code
        and to_date('{string_date}','yyyymmdd') between c.valid_from::date and coalesce(c.valid_until::date,current_date))
        left join operational.m_currency cur on cur.currency_numeric_code = a.transaction_currency_code::numeric
        left join operational.m_currency cur2 on cur2.currency_numeric_code = a.cryptogram_currency_code::numeric
        left join {table_tmp} er
        on (er.date=a.local_transaction_date and er.currency_from_code::numeric=a.transaction_currency_code::numeric and er.currency_to=c.fee_currency and lower(er.brand)='visa')
        left join {table_tmp} er2
        on (er2.date=a.local_transaction_date and er2.currency_from_code::numeric=a.cryptogram_currency_code::numeric and er2.currency_to=c.fee_currency and lower(er2.brand)='visa')
        """
        table_name_tmp = f'{customer_code}_{type_file}_{number_file}_visa_sms_interchange'
        table_tmp = f'{table_scheme_tmp}.{table_name_tmp}'
        
        message_drop = self.ps.drop_table(table_tmp)
        ps_block.drop_table(table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA SMS FILE",
        "INFO",
        message_drop,
        self.module
        )  

        message_create = self.ps.create_table_from_select(query_tmp,table_tmp)
        log.logs().exist_file(
        "OPERATIONAL",
        customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA SMS FILE",
        "INFO",
        message_create,
        self.module
        ) 

        log.logs().exist_file(
        "OPERATIONAL",
        self.customer_code,
        "VISA",
        self.log_name,
        "INTERCHANGE OF VISA SMS FILE",
        "INFO",
        self.ps.insert_from_table(table_scheme_tmp, table_name_tmp,table_scheme, table_name),
        self.module
        )

        if self.debug == 'False':
            bulk_delete = ps_block.query_block()
            log.logs().exist_file(
                "OPERATIONAL",
                customer_code,
                "VISA",
                self.log_name,
                "INTERCHANGE OF VISA SMS FILE",
                "INFO",
                bulk_delete,
                self.module
            )
        return 'finished'

    def fill_range(self,list_value:list)-> list:
        """Generating a fill list
        
        Args:
            list_value (list): list of values 

        Returns:
            list_string (list): list of processed values
        
        """
        new_list = []
        list_string = []
        for value in list_value:
            if str(value).find('-')>0:
                split_value=value.split('-')
                new_list.extend(range(int(split_value[0]),int(split_value[1])+1))
            else:
                new_list.append(int(value))
        list_string = map(str, new_list)
        return list_string
    
    def visa_interchange_rule_assign(self, table_schema_source:str,table_name_source:str,string_date:str)->str:
        """Direct Assignment Exchange Rules
        
        Args:
            table_schema_source (str): schema of table source
            table_name_source (str): table source name
            string_date (str): date filter

        Returns:
            table_name_result (str): Table name as result 

        Process:
            column_exceptions_list (list): It refers to all the columns that exist in the rules table but should be excluded from the exchange engine.     
                                            use example: ['app_id','point_of_service_condition_code']

            column_group_space (list): It refers to all the columns that need to consider space as one of their valid values.  
                                            use example: ['nnss_indicator','cardholder_id_method']
                                            applies to the following rule condition: Y,Space
                                                                                     NOT:Y,Space ("NOT" applies to all the content)
                                                                                     BLANK (if value is null in transactioinal data)

            column_group_amount_currency (list): It refers to all the columns that need to consider the operators greater, lesser, and equal, and amounts with their currencies as one of their valid values.
                                            use example: ['transaction_amount']
                                            applies to the following rule condition: <1000
                                                                                     >0
                                                                                     <=50
                                                                                     >=100
                                                                                     BETWEEN 0 AND 10
                                                                                     10

            column_group_greater_less (list): It refers to all the columns that need to consider the operators greater, lesser, and equal as one of their valid values.
                                            use example: ['surcharge_amount','timeliness']
                                            applies to the following rule condition: <1000
                                                                                     >0
                                                                                     <=50
                                                                                     >=100
                                                                                     BETWEEN 0 AND 10
                                                                                     10

            column_group_number_between (list): It refers to all the columns that need to consider the range operator as one of their valid values.
                                            use example: ['merchant_category_code','issuer_bin_8']
                                            applies to the following rule condition: 0-100,150
                                                                                     NOT:0-100,200 ("NOT" applies to all the content)
                                                                                     
            If the column is not set in the previous groups, the following rule condition will be applied:
                                                                                     BLANK (if value is null or empty spaces in transactional data)
                                                                                     NOT:A,100 ("NOT" applies to all the content)
                                                                                     A,100,200,ABC
                                                                                     0000

        """
        table = f'{table_schema_source.lower()}.{table_name_source.lower()}'
        dict_table = {'app_id':sqlalchemy.Numeric}
        table_name_result = f'{table_name_source.lower()}_assign'
        table_work = pd.DataFrame(self.ps.select(table))
        column_exceptions_list = ['processing_code_transaction_type','point_of_service_condition_code','app_id','intelica_id_original','app_creation_user','app_creation_date','key','transaction_amount_currency','jurisdiction','region_country_code','guide_date','valid_from','valid_until','fee_program','intelica_id','fpi','fee_descriptor','fee_description','cod_hierarchy','program_default','fee_currency','fee_variable','fee_fixed','fee_min','fee_cap','acquiring_identifier','message_identifier','validation_code','v_i_p_full_financial_message_sets','sender_data','additional_sender_data','settlement_service','other_criteria_applies']
        column_group_space = ['nnss_indicator','cardholder_id_method','moto_eci_indicator','acceptance_terminal_indicator','merchant_vat']
        column_group_amount_currency = ['transaction_amount']
        column_group_greater_less = ['surcharge_amount','timeliness']
        column_group_number_between = ['merchant_category_code','issuer_bin_8']
        if table_work.empty:
            query_tmp = f'select null app_id,null app_hash_file,null "rule",null region_country_code,null intelica_id,null cod_hierarchy,null valid_from from {table}'
            message_drop = self.ps.drop_table(f'{table_schema_source}.{table_name_result}')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            message_drop,
            self.module
            )  
            message_create =  self.ps.create_table_from_select(query_tmp,f'{table_schema_source}.{table_name_result}')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA FILE",
            "INFO",
            message_create,
            self.module
            )  

            return table_name_result
        table_work['rule'] = 0
        table_work['id'] = table_work.index + 1
        table_work['product_id'] = table_work['product_id'].str.strip()
        table_rules = pd.DataFrame(self.ps.select('operational.m_interchange_rules_visa r',f"where to_date('{string_date}','yyyymmdd') between r.valid_from::date and coalesce(r.valid_until::date,current_date) and region_country_code in (select jurisdiction from {table} group by 1) order by region_country_code,intelica_id::numeric"))
        table_rules['key'] = table_rules.index + 1
        count_row=0
        count_condition = 0
        for rule in table_rules['key'].values:
            table_test = pd.DataFrame()
            table_test = table_test.join(table_work[(table_work['jurisdiction'] == table_rules['region_country_code'][count_row]) & (table_work['rule'] == 0)],how='right')
            for condition in table_rules.columns.values:
                if condition not in column_exceptions_list:
                    value_condition = str(table_rules[condition][count_row]).replace(' ','')
                    if value_condition.lower().replace('nan','none')  != 'none':
                        if len(table_test)>0:
                            try:

                                if condition in column_group_space:
                                    value_condition = value_condition.lower().replace('space',' ').upper()
                                    if value_condition.find('NOT:')>=0:
                                        value_condition.replace('NOT:','')
                                        split_condition = value_condition.split(',')
                                        table_test = table_test[~table_test[condition].astype(str).isin(split_condition)]
                                    else:
                                        split_condition = value_condition.split(',')
                                        table_test = table_test[table_test[condition].astype(str).isin(split_condition)]
                                    continue
                                if condition in column_group_amount_currency:
                                    table_test = table_test[table_test[condition].astype(str) != 'BLANK']
                                    value_condition_currency = f"{condition}_{str(table_rules['transaction_amount_currency'][count_row]).lower()}"
                                    table_test = table_test[table_test[value_condition_currency].astype(str) != 'BLANK']
                                    table_test[value_condition_currency] = pd.to_numeric(table_test[value_condition_currency])
                                    if value_condition.find('<')>=0 or value_condition.find('>')>=0 or value_condition.find('=')>=0:
                                        query_condition = f"{value_condition_currency} {value_condition.replace('<=','<= ').replace('>=','>= ').replace('>','> ').replace('<','< ')}"
                                        table_test.query(query_condition,inplace=True)
                                    elif value_condition.lower().find('between')>=0:
                                        split_condition = value_condition.lower().replace(' ','').replace('between','').split('and')
                                        table_test=table_test[table_test[value_condition_currency].astype(float).between(int(split_condition[0]),int(split_condition[1]),inclusive=True)]
                                    else:
                                        table_test=table_test[table_test[value_condition_currency].astype(str).str.strip() == value_condition]
                                    continue
                                if condition in column_group_greater_less:
                                    table_test = table_test[table_test[condition] != 'BLANK']
                                    table_test[condition] = pd.to_numeric(table_test[condition])
                                    if value_condition.find('<')>=0 or value_condition.find('>')>=0 or value_condition.find('=')>=0:
                                        query_condition = f"{condition} {value_condition.replace('<=','<= ').replace('>=','>= ').replace('>','> ').replace('<','< ')}"
                                        table_test.query(query_condition,inplace=True)
                                    elif value_condition.lower().find('between')>=0:
                                        split_condition = value_condition.lower().replace(' ','').replace('between','').split('and')
                                        table_test=table_test[table_test[condition].astype(float).between(int(split_condition[0]),int(split_condition[1]),inclusive=True)]
                                    else:
                                        table_test=table_test[table_test[condition] == float(value_condition)]
                                    continue
                                if condition in column_group_number_between:
                                    if value_condition.find('NOT:')>=0:
                                        value_condition = value_condition.replace('NOT:','').strip()
                                        split_condition = value_condition.split(',')
                                        if len(split_condition)<2 and str(value_condition).find('-')<0:
                                            table_test=table_test[table_test[condition].astype(str).str.strip() != value_condition]
                                        else:
                                            table_test=table_test[~table_test[condition].isin(self.fill_range(split_condition))]
                                    else:
                                        split_condition = value_condition.split(',')
                                        if len(split_condition)<2 and str(value_condition).find('-')<0:
                                            table_test=table_test[table_test[condition].astype(str).str.strip() == value_condition]
                                        else:
                                            table_test=table_test[table_test[condition].isin(self.fill_range(split_condition))]
                                    continue
                                if value_condition.find('NOT:')>=0:
                                    value_condition = value_condition.replace('NOT:','').strip()
                                    split_condition = value_condition.split(',')
                                    table_test.loc[table_test[condition].astype(str).str.strip().str.len() == 0, condition] = 'BLANK'
                                    if len(split_condition)<2:
                                        if value_condition.find('.')>=0:
                                            if value_condition.replace('.','').isnumeric():
                                                value_condition = str(int(float(value_condition)))
                                        table_test = table_test[table_test[condition].astype(str).str.strip() != value_condition]
                                    else:
                                        table_test = table_test[~table_test[condition].astype(str).str.strip().isin(split_condition)]
                                else:
                                    value_condition = value_condition.upper()
                                    split_condition = value_condition.split(',')
                                    table_test.loc[table_test[condition].astype(str).str.strip().str.len() == 0, condition] = 'BLANK'
                                    if len(split_condition)<2:
                                        if value_condition.find('.')>=0:
                                            if value_condition.replace('.','').isnumeric():
                                                value_condition = str(int(float(value_condition)))
                                        table_test = table_test[table_test[condition].astype(str).str.strip() == value_condition]
                                    else:
                                        value_condition = value_condition.split(',')
                                        table_test = table_test[table_test[condition].astype(str).str.strip().isin(split_condition)]
                                if table_test.empty:
                                    break
                            except ValueError as error:                   
                                log.logs().exist_file(
                                "OPERATIONAL",
                                self.customer_code,
                                "VISA",
                                self.log_name,
                                "ADAPTER OF VISA FILE",
                                "ERROR",
                                "Error has occurred:"+ str(error),
                                self.module
                                )  
            if len(table_test)>0:
                table_work.loc[table_work['id'].isin(table_test['id'].values),'rule'] = rule
                table_work.loc[table_work['id'].isin(table_test['id'].values),'region_country_code'] = table_rules['region_country_code'][count_row]
                table_work.loc[table_work['id'].isin(table_test['id'].values),'intelica_id'] = table_rules['intelica_id'][count_row]
                table_work.loc[table_work['id'].isin(table_test['id'].values),'cod_hierarchy'] = table_rules['cod_hierarchy'][count_row]
                table_work.loc[table_work['id'].isin(table_test['id'].values),'valid_from'] = table_rules['valid_from'][count_row]
            count_row+=1
        if len(table_work)>0:
            message_drop = self.ps.drop_table(f'{table_schema_source}.{table_name_result}')

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "INTERCHANGE OF VISA FILE",
            "INFO",
            message_drop,
            self.module
            )  
            table_work = table_work[['app_id','app_hash_file','rule','region_country_code','intelica_id','cod_hierarchy','valid_from']].applymap(str)
            table_work["intelica_id"] = table_work["intelica_id"].replace("nan",None)
            table_work["region_country_code"] = table_work["region_country_code"].replace("nan",None)

            self.ps.insert_from_df(table_work,table_schema_source.lower(),table_name_result)
            return table_name_result
        return '-1'

    def visa_sms_interchange_rule_assign(self, table_schema_source:str,table_name_source:str,string_date:str)->str:
        """Direct Assignment Exchange Rules
        
        Args:
            table_schema_source (str): schema of table source
            table_name_source (str): table source name
            string_date (str): date filter

        Returns:
            table_name_result (str): Table name as result
        
        Process:
            column_exceptions_list (list): It refers to all the columns that exist in the rules table but should be excluded from the exchange engine.     
                                            use example: ['app_id','point_of_service_condition_code']

            column_group_space (list): It refers to all the columns that need to consider space as one of their valid values.  
                                            use example: ['nnss_indicator','cardholder_id_method']
                                            applies to the following rule condition: Y,Space
                                                                                     NOT:Y,Space ("NOT" applies to all the content)
                                                                                     BLANK (if value is null in transactioinal data)

            column_group_amount_currency (list): It refers to all the columns that need to consider the operators greater, lesser, and equal, and amounts with their currencies as one of their valid values.
                                            use example: ['transaction_amount']
                                            applies to the following rule condition: <1000
                                                                                     >0
                                                                                     <=50
                                                                                     >=100
                                                                                     BETWEEN 0 AND 10
                                                                                     10

            column_group_greater_less (list): It refers to all the columns that need to consider the operators greater, lesser, and equal as one of their valid values.
                                            use example: ['surcharge_amount','timeliness']
                                            applies to the following rule condition: <1000
                                                                                     >0
                                                                                     <=50
                                                                                     >=100
                                                                                     BETWEEN 0 AND 10
                                                                                     10

            column_group_number_between (list): It refers to all the columns that need to consider the range operator as one of their valid values.
                                            use example: ['merchant_category_code','issuer_bin_8']
                                            applies to the following rule condition: 0-100,150
                                                                                     NOT:0-100,200 ("NOT" applies to all the content)
                                                                                     
            If the column is not set in the previous groups, the following rule condition will be applied:
                                                                                     BLANK (if value is null or empty spaces in transactional data)
                                                                                     NOT:A,100 ("NOT" applies to all the content)
                                                                                     A,100,200,ABC
                                                                                     0000
        """
        table = f'{table_schema_source.lower()}.{table_name_source.lower()}'
        dict_table = {'app_id':sqlalchemy.Numeric}
        table_name_result = f'{table_name_source.lower()}_assign'
        table_work = pd.DataFrame(self.ps.select(table))
        column_exceptions_list = ['transaction_code_qualifier','payment_credential_combination','prepaid_card_indicator','business_format_code','type_purchase','authorized_amount','national_tax_indicator','merchant_vat','summary_commodity','message_identifier','app_id','intelica_id_original','app_creation_user','app_creation_date','key','transaction_amount_currency','jurisdiction','region_country_code','guide_date','valid_from','valid_until','fee_program','intelica_id','fpi','fee_descriptor','fee_description','cod_hierarchy','program_default','fee_currency','fee_variable','fee_fixed','fee_min','fee_cap','acquiring_identifier','message_identifier','validation_code','v_i_p_full_financial_message_sets','sender_data','additional_sender_data','settlement_service','other_criteria_applies']
        column_group_space = ['nnss_indicator','cardholder_id_method','moto_eci_indicator','acceptance_terminal_indicator','merchant_vat']
        column_group_amount_currency = ['transaction_amount']
        column_group_greater_less = ['surcharge_amount','timeliness']
        column_group_number_between = ['merchant_category_code','issuer_bin_8']
        if table_work.empty:
            query_tmp = f'select null app_id,null app_hash_file,null "rule",null region_country_code,null intelica_id,null cod_hierarchy,null valid_from from {table}'
            message_drop = self.ps.drop_table(f'{table_schema_source}.{table_name_result}')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA SMS FILE",
            "INFO",
            message_drop,
            self.module
            )  
            message_create =  self.ps.create_table_from_select(query_tmp,f'{table_schema_source}.{table_name_result}')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "ADAPTER OF VISA SMS FILE",
            "INFO",
            message_create,
            self.module
            )  

            return table_name_result
        table_work['rule'] = 0
        table_work['region_country_code'] = [None] * len(table_work.index)
        table_work['region_country_code'] = table_work['region_country_code'].astype(str)
        table_work['intelica_id'] = [None] * len(table_work.index)
        table_work['intelica_id'] = pd.to_numeric(table_work['intelica_id'])
        table_work['cod_hierarchy'] = [None] * len(table_work.index)
        table_work['cod_hierarchy'] = pd.to_numeric(table_work['cod_hierarchy'])
        table_work['valid_from'] = pd.NaT
        table_work['valid_from'] = pd.to_datetime(table_work['valid_from'])
        table_work['id'] = table_work.index + 1
        table_work['product_id'] = table_work['product_id'].str.strip()
        table_rules = pd.DataFrame(self.ps.select('operational.m_interchange_rules_visa r',f"where to_date('{string_date}','yyyymmdd') between r.valid_from::date and coalesce(r.valid_until::date,current_date) and region_country_code in (select jurisdiction from {table} group by 1) order by region_country_code,intelica_id::numeric"))
        table_rules['key'] = table_rules.index + 1
        count_row=0
        count_condition = 0
        for rule in table_rules['key'].values:
            table_test = pd.DataFrame()
            table_test = table_test.join(table_work[(table_work['jurisdiction'] == table_rules['region_country_code'][count_row]) & (table_work['rule'] == 0)],how='right')
            for condition in table_rules.columns.values:
                if condition not in column_exceptions_list:
                    value_condition = str(table_rules[condition][count_row]).replace(' ','')
                    if value_condition.lower().replace('nan','none')  != 'none':
                        if len(table_test)>0:
                            try:

                                if condition in column_group_space:
                                    value_condition = value_condition.lower().replace('space',' ').upper()
                                    if value_condition.find('NOT:')>=0:
                                        value_condition.replace('NOT:','')
                                        split_condition = value_condition.split(',')
                                        table_test = table_test[~table_test[condition].astype(str).isin(split_condition)]
                                    else:
                                        split_condition = value_condition.split(',')
                                        table_test = table_test[table_test[condition].astype(str).isin(split_condition)]
                                    continue
                                if condition in column_group_amount_currency:
                                    table_test = table_test[table_test[condition].astype(str) != 'BLANK']
                                    value_condition_currency = f"{condition}_{str(table_rules['transaction_amount_currency'][count_row]).lower()}"
                                    table_test = table_test[table_test[value_condition_currency].astype(str) != 'BLANK']
                                    table_test[value_condition_currency] = pd.to_numeric(table_test[value_condition_currency])
                                    if value_condition.find('<')>=0 or value_condition.find('>')>=0 or value_condition.find('=')>=0:
                                        query_condition = f"{value_condition_currency} {value_condition.replace('<=','<= ').replace('>=','>= ').replace('>','> ').replace('<','< ')}"
                                        table_test.query(query_condition,inplace=True)
                                    elif value_condition.lower().find('between')>=0:
                                        split_condition = value_condition.lower().replace(' ','').replace('between','').split('and')
                                        table_test=table_test[table_test[value_condition_currency].astype(float).between(int(split_condition[0]),int(split_condition[1]),inclusive=True)]
                                    else:
                                        table_test=table_test[table_test[value_condition_currency].astype(str).str.strip() == value_condition]
                                    continue
                                if condition in column_group_greater_less:
                                    table_test = table_test[table_test[condition] != 'BLANK']
                                    table_test[condition] = pd.to_numeric(table_test[condition])
                                    if value_condition.find('<')>=0 or value_condition.find('>')>=0 or value_condition.find('=')>=0:
                                        query_condition = f"{condition} {value_condition.replace('<=','<= ').replace('>=','>= ').replace('>','> ').replace('<','< ')}"
                                        table_test.query(query_condition,inplace=True)
                                    elif value_condition.lower().find('between')>=0:
                                        split_condition = value_condition.lower().replace(' ','').replace('between','').split('and')
                                        table_test=table_test[table_test[condition].astype(float).between(int(split_condition[0]),int(split_condition[1]),inclusive=True)]
                                    else:
                                        table_test=table_test[table_test[condition] == float(value_condition)]
                                    continue
                                if condition in column_group_number_between:
                                    if value_condition.find('NOT:')>=0:
                                        value_condition = value_condition.replace('NOT:','').strip()
                                        split_condition = value_condition.split(',')
                                        if len(split_condition)<2 and str(value_condition).find('-')<0:
                                            table_test=table_test[table_test[condition].astype(str).str.strip() != value_condition]
                                        else:
                                            table_test=table_test[~table_test[condition].isin(self.fill_range(split_condition))]
                                    else:
                                        split_condition = value_condition.split(',')
                                        if len(split_condition)<2 and str(value_condition).find('-')<0:
                                            table_test=table_test[table_test[condition].astype(str).str.strip() == value_condition]
                                        else:
                                            table_test=table_test[table_test[condition].isin(self.fill_range(split_condition))]
                                    continue
                                if value_condition.find('NOT:')>=0:
                                    value_condition = value_condition.replace('NOT:','').strip()
                                    split_condition = value_condition.split(',')
                                    table_test.loc[table_test[condition].astype(str).str.strip().str.len() == 0, condition] = 'BLANK'
                                    if len(split_condition)<2:
                                        if value_condition.find('.')>=0:
                                            if value_condition.replace('.','').isnumeric():
                                                value_condition = str(int(float(value_condition)))
                                        table_test = table_test[table_test[condition].astype(str).str.strip() != value_condition]
                                    else:
                                        table_test = table_test[~table_test[condition].astype(str).str.strip().isin(split_condition)]
                                else:
                                    value_condition = value_condition.upper()
                                    split_condition = value_condition.split(',')
                                    table_test.loc[table_test[condition].astype(str).str.strip().str.len() == 0, condition] = 'BLANK'
                                    if len(split_condition)<2:
                                        if value_condition.find('.')>=0:
                                            if value_condition.replace('.','').isnumeric():
                                                value_condition = str(int(float(value_condition)))
                                        table_test = table_test[table_test[condition].astype(str).str.strip() == value_condition]
                                    else:
                                        value_condition = value_condition.split(',')
                                        table_test = table_test[table_test[condition].astype(str).str.strip().isin(split_condition)]
                                if table_test.empty:
                                    break
                            except ValueError as error:                   
                                log.logs().exist_file(
                                "OPERATIONAL",
                                self.customer_code,
                                "VISA",
                                self.log_name,
                                "ADAPTER OF VISA FILE",
                                "ERROR",
                                "Error has occurred:"+ str(error),
                                self.module
                                )  
            if len(table_test)>0:
                table_work.loc[table_work['id'].isin(table_test['id'].values),'rule'] = rule
                table_work.loc[table_work['id'].isin(table_test['id'].values),'region_country_code'] = table_rules['region_country_code'][count_row]
                table_work.loc[table_work['id'].isin(table_test['id'].values),'intelica_id'] = table_rules['intelica_id'][count_row]
                table_work.loc[table_work['id'].isin(table_test['id'].values),'cod_hierarchy'] = table_rules['cod_hierarchy'][count_row]
                table_work.loc[table_work['id'].isin(table_test['id'].values),'valid_from'] = table_rules['valid_from'][count_row]
            count_row+=1
        if len(table_work)>0:
            message_drop = self.ps.drop_table(f'{table_schema_source}.{table_name_result}')

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "VISA",
            self.log_name,
            "INTERCHANGE OF VISA FILE",
            "INFO",
            message_drop,
            self.module
            )  
            table_work = table_work[['app_id','app_hash_file','rule','region_country_code','intelica_id','cod_hierarchy','valid_from']].applymap(str)
            table_work["intelica_id"] = table_work["intelica_id"].replace("nan",None)
            table_work["region_country_code"] = table_work["region_country_code"].replace("nan",None)
            self.ps.insert_from_df(table_work,table_schema_source.lower(),table_name_result)
            return table_name_result
        return '-1'

    def mastercard_interchange_rule_assign(self, table_schema_source:str,table_name_source:str,string_date:str)->str:
        """Direct Assignment Exchange Rules
        
        Args:
            table_schema_source (str): schema of table source
            table_name_source (str): table source name
            string_date (str): date filter

        Returns:
            table_name_result (str): Table name as result
        
        """
        table = f'{table_schema_source.lower()}.{table_name_source.lower()}'
        dict_table = {'app_id':sqlalchemy.Numeric}
        table_name_result = f'{table_name_source.lower()}_assign'
        table_work = pd.DataFrame(self.ps.select(table))
        if table_work.empty:
            query_tmp = f'select null app_id,null app_hash_file,null "rule",null region_country_code,null intelica_id,null cod_hierarchy,null valid_from, null ird , null issuer_bin_8, null acquirer_bin, null electronic_commerce_indicator_3 from {table}'
            message_drop = self.ps.drop_table(f'{table_schema_source}.{table_name_result}')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            message_drop,
            self.module
            )  
            message_create =  self.ps.create_table_from_select(query_tmp,f'{table_schema_source}.{table_name_result}')
            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "ADAPTER OF MASTERCARD FILE",
            "INFO",
            message_create,
            self.module
            )  

            return table_name_result
        table_work['rule'] = 0
        table_work['region_country_code'] = [None] * len(table_work.index)
        table_work['region_country_code'] = table_work['region_country_code'].astype(str)
        table_work['intelica_id'] = [None] * len(table_work.index)
        table_work['intelica_id'] = pd.to_numeric(table_work['intelica_id'])
        table_work['valid_from'] = pd.NaT
        table_work['valid_from'] = pd.to_datetime(table_work['valid_from'])
        table_work['issuer_bin_8'] = table_work['issuer_bin_8'].astype(str)
        table_work['acquirer_bin'] = table_work['acquirer_bin'].astype(str)
        table_work['electronic_commerce_indicator_3'] = table_work['electronic_commerce_indicator_3'].astype(str)
        table_work['id'] = table_work.index + 1
        table_rules = pd.DataFrame(self.ps.select('operational.m_interchange_rules_mc r',f"where to_date('{string_date}','yyyymmdd') between r.valid_from::date and coalesce(r.valid_until::date,current_date) and (region_country_code,ird) in (select jurisdiction,ird from {table} group by 1,2) order by region_country_code,intelica_id::numeric"))
        table_rules['key'] = table_rules.index + 1
        count_row=0
        count_condition = 0

        column_group_number_between = ("issuer_bin_8", "acquirer_bin", "electronic_commerce_indicator_3", "card_acceptor_business_code")

        for rule in table_rules['key'].values:
            table_test = pd.DataFrame()
            table_test = table_test.join(table_work[(table_work['jurisdiction'] == table_rules['region_country_code'][count_row]) & (table_work['rule'] == 0) & (table_work['ird'] == table_rules['ird'][count_row])],how='right')

            for condition in table_rules.columns.values:
                if condition not in ('app_creation_user','app_creation_date','key','amount_transaction_currency','jurisdiction','region_country_code','guide_date','valid_from','valid_until','fee_category','fee_tier','intelica_id','ird','rate_currency','rate_variable','rate_fixed','rate_min','rate_cap','masterpass_incentive_indicator','tti','additional_data','mastercard_assigned_id'):
                    value_condition = str(table_rules[condition][count_row]).replace(' ','')
                    if value_condition.lower().replace('nan','none')  != 'none':
                        if len(table_test)>0:
                            try:
                                if condition in ('amount_transaction'):
                                    table_test = table_test[table_test[condition].astype(str) != 'BLANK']
                                    value_condition_currency = f"{condition}_{str(table_rules['amount_transaction_currency'][count_row]).lower()}"
                                    table_test = table_test[table_test[value_condition_currency].astype(str) != 'BLANK']
                                    table_test[value_condition_currency] = pd.to_numeric(table_test[value_condition_currency])
                                    if value_condition.find('<')>=0 or value_condition.find('>')>=0 or value_condition.find('=')>=0:
                                        list_value_condition = value_condition.split(',')
                                        for value_str in list_value_condition:
                                            query_condition = f"{value_condition_currency} {value_str.replace('<=','<= ').replace('>=','>= ').replace('>','> ').replace('<','< ')}"
                                            table_test.query(query_condition,inplace=True)
                                    elif value_condition.lower().find('between')>=0:
                                        split_condition = value_condition.lower().replace(' ','').replace('between','').split('and')
                                        table_test=table_test[table_test[value_condition_currency].astype(float).between(int(split_condition[0]),int(split_condition[1]),inclusive=True)]
                                    else:
                                        table_test=table_test[table_test[value_condition_currency].astype(str).str.strip() == value_condition]
                                    continue

                                if condition in column_group_number_between:
                                    if 'NOT:' in value_condition:
                                        value_condition = value_condition.replace('NOT:', '').strip()
                                        split_condition = value_condition.split(',')
                                        if len(split_condition)<2 and str(value_condition).find('-')<0:
                                            table_test = table_test[table_test[condition].astype(str).str.strip() != value_condition]
                                        else:
                                            table_test = table_test[~table_test[condition].isin(self.fill_range(split_condition))]
                                    else:
                                        split_condition = value_condition.split(',')
                                        if len(split_condition)<2 and str(value_condition).find('-')<0:
                                            table_test = table_test[table_test[condition].astype(str).str.strip() == value_condition]
                                        else:
                                            table_test = table_test[table_test[condition].isin(self.fill_range(split_condition))]
                                    continue
                                if value_condition.find('NOT:')>=0:
                                    value_condition = value_condition.replace('NOT:','').strip()
                                    split_condition = value_condition.split(',')
                                    if len(split_condition)<2:
                                        if value_condition.find('.')>=0:
                                            if value_condition.replace('.','').isnumeric():
                                                value_condition = str(int(float(value_condition)))
                                        table_test = table_test[table_test[condition].astype(str).str.strip() != value_condition]
                                    else:
                                        table_test = table_test[~table_test[condition].astype(str).str.strip().isin(split_condition)]
                                else:
                                    value_condition = value_condition.upper()
                                    split_condition = value_condition.split(',')
                                    if len(split_condition)<2:
                                        if value_condition.find('.')>=0:
                                            if value_condition.replace('.','').isnumeric():
                                                value_condition = str(int(float(value_condition)))
                                        table_test = table_test[table_test[condition].astype(str).str.strip() == value_condition]
                                    else:
                                        value_condition = value_condition.split(',')
                                        table_test = table_test[table_test[condition].astype(str).str.strip().isin(split_condition)]
                                if table_test.empty:
                                    break
                            except ValueError as error:
                                log.logs().exist_file(
                                "OPERATIONAL",
                                self.customer_code,
                                "MASTERCARD",
                                self.log_name,
                                "ADAPTER OF MASTERCARD FILE",
                                "ERROR",
                                "Error has occurred:"+ str(error),
                                self.module
                                )  

            if len(table_test)>0:
                table_work.loc[table_work['id'].isin(table_test['id'].values),'rule'] = rule
                table_work.loc[table_work['id'].isin(table_test['id'].values),'region_country_code'] = table_rules['region_country_code'][count_row]
                table_work.loc[table_work['id'].isin(table_test['id'].values),'intelica_id'] = table_rules['intelica_id'][count_row]
                table_work.loc[table_work['id'].isin(table_test['id'].values),'valid_from'] = table_rules['valid_from'][count_row]
            count_row+=1

        if len(table_work)>0:
            message_drop = self.ps.drop_table(f'{table_schema_source}.{table_name_result}')

            log.logs().exist_file(
            "OPERATIONAL",
            self.customer_code,
            "MASTERCARD",
            self.log_name,
            "INTERCHANGE OF MASTERCARD FILE",
            "INFO",
            message_drop,
            self.module
            )  
            self.ps.insert_from_df(table_work[['app_id','app_hash_file','rule','region_country_code','intelica_id','valid_from','ird','issuer_bin_8','acquirer_bin','electronic_commerce_indicator_3']].applymap(str),table_schema_source.lower(),table_name_result)
            return table_name_result
        return '-1' 
