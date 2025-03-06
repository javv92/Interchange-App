import pandas as pd
import numpy as np
import Module.Logs.logs as log


class dq_cleaning:
    def __init__(self,customer_code,log_name,module) -> None:
        self.customer_code = customer_code
        self.log_name = log_name
        self.module = module
        pass

    def dq_cls_omitir_obs(self,df,lst_obs):
        df_filtered = df[~df.index.isin(lst_obs)]
        log.logs().exist_file( "OPERATIONAL",
                        self.customer_code,
                        "MASTERCARD & VISA", 
                        self.log_name,
                        "INTERCHANGE OF MC: DQ-CLEANSING", 
                        "INFO", 
                        "Finally the results after DQ-CLEANSING (rows,columns) is " + str(df_filtered.shape),
                        self.module)
        return df_filtered
    
