import pandas as pd
import yaml
import sys
import os
import numpy as np
import Module.Logs.logs as log
from datetime import datetime

class dq_profilling:
    """Class to execute the DataQuality Module.
    Params*********:
        customer_code (str): customer code.
        log_file: log file name.
    """

    def __init__(self,path_yml:str,customer_code,log_name,module) -> None:

        self.path_yml = path_yml
        self.customer_code = customer_code
        self.log_name = log_name
        self.module = module
        pass

    def load_yml_brands(self,brand,section):
        """
         Load a yml based on a route

         Args:
             file_path (str): Path to the YAML file.

         Returns:
             dict: A dictionary with the expected data types.
         """
        with open(self.path_yml, "r") as archivo:
            body_yml = yaml.safe_load(archivo)
            if section!='' and brand !='':
                body_yml = body_yml[brand][section]
            else:
                log.logs().exist_file(
                    "OPERATIONAL",
                    self.customer_code,
                    "MASTERCARD & VISA", 
                    self.log_name,
                    "INTERCHANGE OF MC: DQ-PROFILING", 
                    "ERROR", 
                    "ERROR : The table definition was not found in the YAML file ",
                    self.module
                )
                return None
        return body_yml
    
    def dq_pf_isdate_julian_yddd(self,date_julian:str):
        # Verificar la longitud de la cadena
        if len(date_julian) != 4:
            return False

        # Obtiene el año actual
        year_now = datetime.now().year
        
        
        # Separar el year (Y) y el día ordinal (DDD)
        year_str = str(year_now)[:3] + date_julian[0]
        ordinal_day_str = date_julian[1:]
        
        # Verificar si el year y el día ordinal son números enteros
        if not (year_str.isdigit() and ordinal_day_str.isdigit()):
            return False
        
        # Convertir a enteros
        year = int(year_str)
        ordinal_day = int(ordinal_day_str)
        
        # Verificar el rango del día ordinal
        if ordinal_day < 1 or ordinal_day > 366:
            return False
        
        # Verificar si el year es bisiesto
        is_bisiesto = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
        
        # Ajustar el rango del día ordinal según si es bisiesto o no
        if not is_bisiesto and ordinal_day > 365:
            return False
        return True

    # Definir una función que genera una excepción si la validación falla
    def validate_or_raise(self,date_julian):
        if not self.dq_pf_isdate_julian_yddd(date_julian):
            raise ValueError(f"Invalid Julian date: {date_julian}")
        return True
    
    def dq_pf_casting_columns_type(self,dataframe,brand,table):
        """
         Validates and converts the data types of the specified columns to a DataFrame.

         Args:
             dataframe (pd.DataFrame): The DataFrame to process.
             expected_types (dict): A dictionary where the keys are the column names and the values are the expected data types.

         Returns:
             pd.DataFrame: The DataFrame with the conversions applied.
             list: List of row indexes that failed to do the data type conversion.
         """
        columnas_especificas= self.load_yml_brands(brand,table)
        result_no_convertidas = []
        passed = True
        message=""
        filas_no_convertidas = []
        dataframe.replace('', pd.NA, inplace=True)
        for columna in columnas_especificas:
            col_name = columna["name"]
            col_type = columna["tipo"]
            col_format = columna["format"]
            if col_name not in dataframe.columns:
                log.logs().exist_file(
                    "OPERATIONAL",
                    self.customer_code, 
                    "MASTERCARD & VISA",
                    self.log_name,
                    "INTERCHANGE OF MC: DQ-PROFILING", 
                    "WARNING",
                    f"The column '{col_name}' is not in the dataframe ", 
                    self.module 
                )
                continue
            try:
                dataframe.dropna(subset=col_name,inplace=True)
                if (len(dataframe)>0):
                    if col_type.lower() in ['date_julian']:
                        validacion = lambda x: self.dq_pf_isdate_julian_yddd(x)
                        dataframe[col_name] = dataframe[col_name].apply(self.validate_or_raise)
                    if col_type.lower() in ['date']:
                        validacion = lambda x: pd.isna(x) or (pd.to_datetime(x, format=col_format, errors='coerce') is not pd.NaT)
                        pd.to_datetime(dataframe[col_name], format=col_format, errors='raise')
                    if col_type.lower() in ['int']:
                        validacion= lambda x: pd.isna(x) or (isinstance(x, int) or (isinstance(x, str) and x.strip().isdigit()))
                        dataframe[col_name].astype(col_type)
                    if col_type.lower() in ['float']:
                        validacion= lambda x: pd.isna(x) or (isinstance(x, float) or (isinstance(x, str) and x.strip().replace('.', '', 1).isdigit()))
                        dataframe[col_name].astype(col_type)
                else:
                    log.logs().exist_file(
                        "OPERATIONAL",
                        self.customer_code, 
                        "MASTERCARD & VISA",
                        self.log_name,
                        "INTERCHANGE OF MC: DQ-PROFILING", 
                        "WARNING",
                        f"The column '{col_name}' don't have elements with value distinct than NULL.", 
                        self.module 
                    )
                passed = True

            except (ValueError,TypeError) as e:
                message = f"Error to convert '{col_name}' in type {col_type} || {e}"
                error_indices = dataframe[dataframe[col_name].apply(lambda x: not validacion(x))].index
                passed=False
                filas_no_convertidas.extend(error_indices)
                tupla= (error_indices,message)
                result_no_convertidas.append(tupla)
        if (filas_no_convertidas and len(filas_no_convertidas)>0):
            passed = False
        return passed, result_no_convertidas,filas_no_convertidas

    def dq_pf_df_observed(self,df,rows_nopassed,lst_df_obs):
        lst_df_obs = []
        for tupla in  rows_nopassed:
            df_nopassed = df.loc[tupla[0]]   
            df_nopassed['message_error'] = tupla[1]
            lst_df_obs.append(df_nopassed)

        df_obs = pd.concat(lst_df_obs, ignore_index=True) 
        df_obs.columns= df_obs.columns.str.strip().str.replace('[', '', regex=False) \
                  .str.replace(']', '', regex=False) \
                  .str.replace('/', '_', regex=False) \
                  .str.replace('(', '', regex=False) \
                  .str.replace(')', '', regex=False).str.replace(' ', '_', regex=False) 
        return df_obs
