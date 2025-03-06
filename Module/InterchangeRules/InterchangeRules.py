import argparse
from ast import Raise
from cmath import isnan
from errno import errorcode
from zlib import DEF_BUF_SIZE
import time
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from os import remove
from pathlib import Path
from collections import Counter
import sys

path = Path(__file__).resolve().parent.parent.parent
sys.path.append(os.path.join(os.getcwd(), str(path)))

from Module.Persistence.connection import connect_to_postgreSQL as bdpostgre
from Module.Persistence.connection import connect_to_s3 as s3
import Module.Logs.logs as log

class interchangeRules:
    """Class to manage the interchange rules from bucket for both brands"""
    load_dotenv()
    def __init__(self) -> None:
        path =  r"/home/ec2-user/interchange"
        self.route_visa = path + "/Build/VISA Reglas Intercambio.xlsx" 
        self.route_mc =  path + "/Build/MASTERCARD Reglas Intercambio.xlsx" 
        self.schema = 'operational'
        self.tmp_schema = 'temporal'
        self.table_operat_mc = 'm_interchange_rules_mc'
        self.table_operat_visa = 'm_interchange_rules_visa'
        self.bucket = os.getenv("LANDING_BUCKET") 
        self.module = "INTERCHANGERULES"
        pass
    
    def validate_columns_file_visa(self,columns : list, name_log : str)-> str:
        """Validte excel file columns difference 

        Args:
            columns (list): list of columns for validation  

        Returns:
            str: result message.

        """
        list_keys = ['jurisdiction','guide_date','valid_from','intelica_id']
        list_numeric = ['fee_fixed','fee_min','fee_cap','fee_variable']
        list_date = ['guide_date']
        list_timestamp = ['valid_from','valid_until','app_creation_date']
        query_text=f'create table {self.tmp_schema}.{self.table_operat_visa} ('
        df_columns = pd.DataFrame(bdpostgre().get_structure_table_from_db(self.schema,self.table_operat_visa))
        columns.append('app_creation_date')
        columns.append('app_creation_user')
        result = 'New columns not found'
        if Counter(columns) != Counter(df_columns['column_name'].values.tolist()):
            for value in columns:
                query_text += value
                if value in list_date:
                    query_text+=' date'
                if value in list_numeric:
                    query_text+=' numeric(24,6)'
                if value in list_timestamp:
                    query_text+=' timestamp'
                if value not in list_date+list_timestamp+list_numeric:
                    query_text+=' text'
                if value in list_keys:
                    query_text+=' not'
                query_text+=' null'
                if value in ['app_creation_date']:
                    query_text+=' DEFAULT CURRENT_TIMESTAMP'
                if value in ['app_creation_user']:
                    query_text+=' DEFAULT USER'
                query_text+=','
            query_text=query_text[:-1]+')'
            result = 'Recreated table.'
            try:
                result_msg = bdpostgre().drop_table(f'{self.tmp_schema}.{self.table_operat_visa}')
                result_msg = bdpostgre().execute_block(query_text)
                query_rename = f'alter table {self.tmp_schema}.{self.table_operat_visa} set schema {self.schema}'
                result_msg = bdpostgre().drop_table(f'{self.schema}.{self.table_operat_visa}')
                result_msg = bdpostgre().execute_block(query_rename)
                result_msg = bdpostgre().create_table_index(self.schema,self.table_operat_visa,'m_interchange_rules_visa_applied_id',"region_country_code, valid_from, COALESCE(valid_until, '9999-12-31 00:00:00'::timestamp without time zone), intelica_id")
                result_msg = bdpostgre().create_table_index(self.schema,self.table_operat_visa,'m_interchange_rules_visa_idx',"jurisdiction, guide_date, intelica_id")
            except Exception as e:
                log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES PROCESS ",
                        "CRITICAL",
                        f'Structure error: {e} ',
                        self.module,
                    )
                sys.exit()
        
        return result
    
    def read_file_visa(self,route:str)-> pd.DataFrame:
        """Reads an excel file with visa format

        Args:
            route (str): path to excel file in local  

        Returns:
            file (DataFrame): DataFrame with excel info.

        """
        file = pd.read_excel(route, sheet_name = "Visa", dtype = str,skiprows = 3)
        return file

    def read_file_mc(self,route:str)-> pd.DataFrame:
        """Reads an excel file with mastercard format

        Args:
            route (str): path to excel file in local  

        Returns:
            file (DataFrame): DataFrame with excel info.

        """
        file = pd.read_excel(route, sheet_name = "Mastercard ITX & Service", dtype = str,skiprows = 1)
        return file

    def block_update_table_visa(self,schema:str,tmp_schema:str,table:str,engine)-> pd.DataFrame:
        """get visa tables to update

        Args:
            schema (str): main schema name
            tmp_schema (str): temporal schema name
            table (str): table name
            engine : connection object

        Returns:
            tmp_final (DataFrame): dataframe with data
        """

        tmp1 = self.read_table(schema,table,engine)
        tmp2 = self.read_table(tmp_schema,table,engine)
        tmp1['key'] = tmp1['jurisdiction'] + tmp1['guide_date'].astype(str) + tmp1['valid_from'].astype(str) + tmp1['intelica_id']  + tmp1['cod_hierarchy']
        left_key = tmp1['key'].unique().tolist()
        tmp2['key'] = tmp2['jurisdiction'] + tmp2['guide_date'].astype(str) + tmp1['valid_from'].astype(str)  + tmp2['intelica_id'] + tmp1['cod_hierarchy']
        right_key = tmp2['key'].unique().tolist()
        tmp_left = tmp1[~tmp1.key.isin(right_key)].drop(columns= ['key'])
        tmp_inner = tmp1.merge(tmp2[['jurisdiction','guide_date','valid_from','intelica_id','cod_hierarchy']],how = 'inner', on = ['jurisdiction','guide_date','valid_from','intelica_id','cod_hierarchy']).drop(columns= ['key'])
        tmp_right = tmp2[~tmp2.key.isin(left_key)].drop(columns= ['key'])
        tmp_final = pd.concat([tmp_left,tmp_inner,tmp_right])
        return tmp_final

    def block_update_table_mc(self,schema:str,tmp_schema:str,table:str,engine):
        """get mastercard tables to update

        Args:
            schema (str): main schema name
            tmp_schema (str): temporal schema name
            table (str): table name
            engine : connection object

        Returns:
            tmp_final (DataFrame): dataframe with data
        """
        tmp1 = self.read_table(schema,table,engine)
        tmp2 = self.read_table(tmp_schema,table,engine)
        tmp1['key'] = tmp1['jurisdiction'] + tmp1['guide_date'].astype(str) + tmp1['valid_from'].astype(str) + tmp1['intelica_id'] 
        left_key = tmp1['key'].unique().tolist()
        tmp2['key'] = tmp2['jurisdiction'] + tmp2['guide_date'].astype(str) + tmp1['valid_from'].astype(str)  + tmp2['intelica_id'] 
        right_key = tmp2['key'].unique().tolist()
        tmp_left = tmp1[~tmp1.key.isin(right_key)].drop(columns= ['key'])
        tmp_inner = tmp1.merge(tmp2[['jurisdiction','guide_date','valid_from','intelica_id']],how = 'inner', on = ['jurisdiction','guide_date','valid_from','intelica_id']).drop(columns= ['key'])
        tmp_right = tmp2[~tmp2.key.isin(left_key)].drop(columns= ['key'])
        tmp_final = pd.concat([tmp_left,tmp_inner,tmp_right])
        return tmp_final

    def read_table(self,schema:str,table:str,engine)->pd.DataFrame:
        """Reads a table

        Args:
            schema (str): main schema name
            table (str): table name
            engine : connection object

        Returns:
            dataframe (DataFrame): dataframe with data
        """
        dataframe = pd.read_sql("select * from %s.%s"%(schema,table),engine)
        return dataframe


    def read_rules_visa(self,dataframe:pd.DataFrame, name_log:str):
        """VISA Rules Reader
        
        Args:
            dataframe (DataFrame): DataFrame with data.
            name_log (str): name of log file.

        Returns:
            data (DataFrame): dataframe with data processed.

        """
        dataframe = dataframe.rename(columns={'DYNAMIC_CURRENCY_INDICATOR':'DYNAMIC_CURRENCY_CONVERSION_INDICATOR'})
        data = dataframe.copy()        
        try:
            if(data['JURISDICTION'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES PROCESS ",
                        "CRITICAL",
                        "FIELD JURISDICTION IS NULL",
                        self.module,
                    )
            return None
        try:
            if(data['GUIDE_DATE'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES PROCESS ",
                        "CRITICAL",
                        "FIELD GUIDE_DATE IS NULL",
                        self.module,
                    )
            return None
        try:
            if(data['VALID_FROM'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD VALID_FROM IS NULL",
                        self.module,
                    )
            return None
        try:
            if(data['FEE_PROGRAM'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD FEE_PROGRAM IS NULL",
                        self.module,
                    )
            return None
        try:
            if(data['INTELICA_ID'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD INTELICA_ID IS NULL",
                        self.module,
                    )
            return None
        try:
            if(data['FPI'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD FPI IS NULL",
                        self.module,
                    )
            return None
        try:
            if(data['FEE_DESCRIPTOR'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD FEE_DESCRIPTOR IS NULL",
                        self.module,
                    )
            return None
        try:
            if(data['FEE_DESCRIPTION'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD FEE_DESCRIPTION IS NULL",
                        self.module,
                    )
            return None
        try:
            if(data['COD_HIERARCHY'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD COD_HIERARCHY IS NULL",
                        self.module,
                    )
            return None
        try:
            data['FEE_FIXED'] = data['FEE_FIXED'].apply(lambda x: float(x))
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "not numeric field: FEE_FIXED!",
                        self.module,
                    )
            return None
        try:
            data['FEE_MIN'] = data['FEE_MIN'].apply(lambda x: float(x))
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "not numeric field: FEE_MIN!",
                        self.module,
                    )
            return None
        try:
            data['FEE_CAP'] = data['FEE_CAP'].apply(lambda x: float(x))
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "not numeric field: FEE_CAP!",
                        self.module,
                    )
            return None
        try:
            data['FEE_VARIABLE'] = data['FEE_VARIABLE'].apply(lambda x: float(x))
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "not numeric field: FEE_VARIABLE!",
                        self.module,
                    )
            return None

        data = data.copy()

        data.columns = data.columns.str.lower()

        return data

    def read_rules_mc(self,dataframe, name_log):
        """MASTERCARD rules reader
        
        Args:
            dataframe (DataFrame): DataFrame with data.
            name_log (str): name of log file.

        Returns:
            data (DataFrame): dataframe with data processed.
        
        """
        
        data = dataframe[['JURISDICTION','REGION_COUNTRY_CODE','GUIDE_DATE', 'VALID_FROM', 'VALID_UNTIL', 'CATEGORY', 'PAYMENT_PRODUCT', 
        'FEE_TIER', 'INTELICA_ID', 'IRD', 'RATE_CURRENCY', 'RATE_VARIABLE', 'RATE_FIXED', 'RATE_MIN', 'RATE_CAP', 
        'PROCESSING_CODE', 'AMOUNT_TRANSACTION_CURRENCY', 'AMOUNT_TRANSACTION', 'CARD_ACCEPTOR_BUSINESS_CODE', 
        'GCMS_PRODUCT_IDENTIFIER', 'FUNDING_SOURCE', 'MASTERPASS_INCENTIVE_INDICATOR', 'MASTERCARD_ASSIGNED_ID', 
        'TTI', 'ADDITIONAL_DATA','ISSUER_BIN_8','ACQUIRER_BIN']].copy()

        try:
            if(data['JURISDICTION'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD JURISDICTION IS NULL",
                        self.module,
                    )
            return None

        try:
            if(data['GUIDE_DATE'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD GUIDE_ATE IS NULL",
                        self.module,
                    )
            return None
            
        try:
            if(data['VALID_FROM'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD JURISDICTION IS NULL",
                        self.module,
                    )
            return None
            
        try:
            if(data['CATEGORY'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD CATEGORY IS NULL",
                        self.module
                    )
            return None
            
        try:
            if(data['FEE_TIER'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD FEE_TIER IS NULL",
                        self.module
                    )
            return None
            
        try:
            if(data['INTELICA_ID'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD INTELICA_ID IS NULL",
                        self.module
                    )
            return None
           
        try:
            if(data['IRD'].isnull().values.any()):
                raise
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "FIELD IRD IS NULL",
                        self.module,
                    )
            return None

        try:
            data['RATE_FIXED'] = data['RATE_FIXED'].apply(lambda x: float(x))
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "not numeric field: RATE_FIXED!",
                        self.module
                    )
            return None

        try:
            data['RATE_MIN'] = data['RATE_MIN'].apply(lambda x: float(x))
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "not numeric field: RATE_MIN!",
                        self.module
                    )
            return None

        try:
            data['RATE_CAP'] = data['RATE_CAP'].apply(lambda x: float(x))
        except:
            log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "CRITICAL",
                        name_log,
                        "INTERCHANGE RULES ",
                        "CRITICAL",
                        "not numeric field: RATE_CAP!",
                        self.module
                    )
            return None

        data.columns = ['jurisdiction','region_country_code' ,'guide_date', 'valid_from', 'valid_until', 'category', 'payment_product', 
        'fee_tier', 'intelica_id', 'ird', 'rate_currency', 'rate_variable', 'rate_fixed', 'rate_min', 'rate_cap', 
        'processing_code', 'amount_transaction_currency', 'amount_transaction', 'card_acceptor_business_code', 
        'gcms_product_identifier', 'funding_source', 'masterpass_incentive_indicator', 'mastercard_assigned_id', 
        'tti', 'additional_data', 'issuer_bin_8', 'acquirer_bin']

        data['fee_category'] = data['category'] + " " + data['payment_product']

        data = data[['jurisdiction', 'region_country_code','guide_date', 'valid_from', 'valid_until', 'fee_category', 
        'fee_tier', 'intelica_id', 'ird', 'rate_currency', 'rate_variable', 'rate_fixed', 'rate_min', 'rate_cap', 
        'processing_code', 'amount_transaction_currency', 'amount_transaction', 'card_acceptor_business_code', 
        'gcms_product_identifier', 'funding_source', 'masterpass_incentive_indicator', 'mastercard_assigned_id', 
        'tti', 'additional_data', 'issuer_bin_8', 'acquirer_bin']].copy()
        data.fee_tier = data.fee_tier.fillna('S/I')

        return data

    def update_master_interchange(self,types:str, brand:str):
        """Main method of execution for interchange rules.
        
        Args:
            types (str): type of execution.
            brand (str): brand of file.
        
        """
        
        engine = bdpostgre().prepare_engine()
        conn = engine.connect()
        br = "VISA" if brand == "VI" else "MASTERCARD"
        if types == 'TI':

            name_log = log.logs().new_log("MASTER", "INTERCHANGE RULES", 'INTELICA', '', br,self.module)

            if(brand == 'VI'):  
                bol = s3().get_object(self.bucket, "Intelica/INTERCHANGE_RULES/VISA/VISA Reglas Intercambio.xlsx", self.route_visa)
                log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "STARTING PROCESS OF NEW MASTER DATA!",
                        self.module,
                    )

                if(bol == True):
                    data = self.read_file_visa(self.route_visa)
                    data_ingest = self.read_rules_visa(data,name_log)
                    if not data_ingest is None:
                        result_msg = self.validate_columns_file_visa(data_ingest.columns.to_list(),name_log)

                    if(data_ingest is None):
                        log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "CLOSING PROCESS FOR FAIL!",
                            self.module,
                        )
                        sys.exit()
                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "VALIDATE COLUMNS",
                            self.module,
                    )
                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            result_msg,
                            self.module,
                    )
                    truncate = "TRUNCATE " + self.schema + "." + self.table_operat_visa
                    conn.execute(truncate)

                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "TRUNCATE TABLE VISA!",
                            self.module,
                    )

                    data_ingest.to_sql(schema = self.schema, name = self.table_operat_visa, con = engine, if_exists = 'append', chunksize = 1000, index= False)

                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "INSERTING REAL TABLE!",
                        self.module,
                        )
           
                    remove(self.route_visa) 
                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "REMOVING LOCAL FILE!",
                            self.module,
                    )

                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "PROCESS FINISHED SUCCESSFULLY!",
                            self.module,
                    )

                else:
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "WARNING",
                        "procesoss aborted: verify that the file is uploaded to s3!!",
                        self.module,
                    )
                    
            elif(brand == 'MC'): 
                bol = s3().get_object(self.bucket, "Intelica/INTERCHANGE_RULES/MASTERCARD/MASTERCARD Reglas Intercambio.xlsx", self.route_mc)
                log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "INFO",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "STARTING PROCESS OF NEW MASTER DATA!",
                        self.module,
                    )       
                
                if(bol == True):
                   
                    data = self.read_file_mc(self.route_mc)
                    data_ingest = self.read_rules_mc(data, name_log)

                    if(data_ingest is None):
                        log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "INFO",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "CLOSING PROCESS FOR FAIL!",
                            self.module,
                        )
                        sys.exit()
                    truncate = "TRUNCATE " + self.schema + "." + self.table_operat_mc
                    conn.execute(truncate)

                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "TRUNCATE TABLE MC!",
                            self.module,
                    )
                    data_ingest.to_sql(schema = self.schema, name = self.table_operat_mc, con = engine, if_exists = 'append', chunksize = 1000, index= False)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "INFO",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "INSERTING REAL TABLE!",
                        self.module,
                    )
                        
                    remove(self.route_mc)
                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "INFO",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "REMOVING LOCAL FILE!",
                            self.module,
                    )

                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "INFO",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "PROCESS FINISHED SUCCESSFULLY!",
                            self.module,
                    )

                else:
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "WARNING",
                        name_log,
                        "INTERCHANGE RULES ",
                        "WARNING",
                        "procesoss aborted: verify that the file is uploaded to s3!!",
                        self.module,
                    )
                
            conn.close()
            engine.dispose()
                           
        elif types == 'U':
            
            name_log = log.logs().new_log("MASTER", "INTERCHANGE RULES", 'INTELICA', '', brand,self.module)

            if(brand == 'VI'):
                bol = s3().get_object(self.bucket, "Intelica/INTERCHANGE_RULES/VISA/VISA Reglas Intercambio.xlsx", self.route_visa)
                if(bol == True):
                    truncate = "TRUNCATE " + self.tmp_schema + "." + self.table_operat_visa
                    conn.execute(truncate)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "TRUNCATE TEMPORAL TABLE!",
                        self.module,
                    )

                    data = self.read_file_visa(self.route_visa)
                    data_ingest = self.read_rules_visa(data,name_log)

                    if(data_ingest is None):
                        log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "CLOSING PROCESS FOR FAIL!",
                            self.module,
                        )
                        sys.exit()
                    
                    data_ingest.to_sql(schema =self.tmp_schema, name = self.table_operat_visa, con = engine, if_exists = 'append', chunksize = 1000, index= False)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "INSERTING TEMPORAL TABLE!",
                        self.module,
                    )

                    data_update = self.block_update_table_visa(self.schema,self.tmp_schema,self.table_operat_visa,conn)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "MERGE OPERATIONAL TABLE VS TEMPORARY TABLE FINISH!",
                        self.module,
                    )

                    truncate = "TRUNCATE " + self.schema + "." + self.table_operat_visa
                    conn.execute(truncate)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "TRUNCATE OPERATIONAL TABLE!",
                        self.module,
                    )
                    data_ingest.to_sql(schema =self.schema, name = self.table_operat_visa, con = engine, if_exists = 'append', chunksize = 1000, index= False)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "INSERTING OPERATIONAL TABLE!",
                        self.module,
                    )
                    remove(self.route_visa) 
                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "REMOVING LOCAL FILE!",
                            self.module,
                        )
                    
                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "PROCESS FINISHED SUCCESSFULLY!",
                            self.module,
                        )

                else:
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "VISA",
                        name_log,
                        "INTERCHANGE RULES ",
                        "WARNING",
                        "PROCESS ABORTED: VERIFY THAT THE FILE IS UPLOADED TO S3!!",
                        self.module,
                    )

            elif(brand == 'MC'):
                bol = s3().get_object(self.bucket, "Intelica/INTERCHANGE_RULES/MASTERCARD/MASTERCARD Reglas Intercambio.xlsx", self.route_mc)
                if(bol == True):
                    truncate = "TRUNCATE " + self.tmp_schema + "." + self.table_operat_mc
                    conn.execute(truncate)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "MASTERCARD",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "TRUNCATE TEMPORAL TABLE!",
                        self.module,
                    )

                    data = self.read_file_mc(self.route_mc)
                    data_ingest = self.read_rules_mc(data,name_log)

                    if(data_ingest is None):
                        log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "VISA",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "CLOSING PROCESS FOR FAIL!",
                            self.module,
                        )
                        sys.exit()
                    
                    data_ingest.to_sql(schema =self.tmp_schema, name = self.table_operat_mc, con = engine, if_exists = 'append', chunksize = 1000, index= False)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "MASTERCARD",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "INSERTING TEMPORAL TABLE!",
                        self.module,
                    )

                    data_update = self.block_update_table_mc(self.schema,self.tmp_schema,self.table_operat_mc,conn)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "MASTERCARD",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "MERGE OPERATIONAL TABLE VS TEMPORARY TABLE FINISH!",
                        self.module,
                    )

                    truncate = "TRUNCATE " + self.schema + "." + self.table_operat_mc
                    conn.execute(truncate)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "MASTERCARD",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "TRUNCATE OPERATIONAL TABLE!",
                        self.module,
                    )
                    data_ingest.to_sql(schema =self.schema, name = self.table_operat_mc, con = engine, if_exists = 'append', chunksize = 1000, index= False)
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "MASTERCARD",
                        name_log,
                        "INTERCHANGE RULES ",
                        "INFO",
                        "INSERTING OPERATIONAL TABLE!",
                        self.module,
                    )
                    remove(self.route_mc) 
                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "MASTERCARD",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "REMOVING LOCAL FILE!",
                            self.module,
                        )
                    
                    log.logs().exist_file(
                            "MASTER",
                            "INTELICA",
                            "MASTERCARD",
                            name_log,
                            "INTERCHANGE RULES ",
                            "INFO",
                            "PROCESS FINISHED SUCCESSFULLY!",
                            self.module,
                        )

                else:
                    log.logs().exist_file(
                        "MASTER",
                        "INTELICA",
                        "MASTERCARD",
                        name_log,
                        "INTERCHANGE RULES ",
                        "WARNING",
                        "PROCESS ABORTED: VERIFY THAT THE FILE IS UPLOADED TO S3!!",
                        self.module,
                    )


            conn.close()
            engine.dispose()

        return None