import os
class getquery():
    """Class to store querys
    
    Params:
        client (str): client code.
    
    """

    def __init__(self,client:str) -> None:
        self.client = client
        pass

    def get_detail_columns(self)->list:
        """Return list of columns to insert/read of the detail of report
        
        Returns:
            list: list of columns.
        """
        return  [
            'app_type_file',
            'app_customer_code' ,
            'app_execution_id' ,
            'scheme_fee_execution_month',
            'app_id' ,
            'app_hash_file' ,
            'table_description' ,
            'app_processing_date' ,
            'account_number' ,
            'card_acceptor_id' ,
            'account_funding_source' ,
            'account_range_country' ,
            'product_id' ,
            'range_program_id' ,
            'jurisdiction' ,
            'business_transaction_type_id' ,
            'reversal_indicator' ,
            'currency_local_indicator',
            'motoec_indicator' ,
            'transaction_count' ,
            'settlement_amount' ,
            'transaction_scheme_fee_cost' ,
            'transaction_brand' ,
            'merchant_country_code',
            'switch_code' ,
            'settlement_currency' ,
            'bank_country' ,
            'business_mode_id' ,
            'size_ticket' ,
            'unitary_scheme_fee_cost' ,
            'estimated_scheme_fee_cost' ,
            'unitary_estimated_scheme_fee_cost' ,
            'transaction_purchase_date',
            'exchange_rate',
            'report_amount',
            'report_currency',
        ]
    
    def get_report_columns(self)->list:
        """Return list of columns to insert/read  of report
        
        Returns:
            list: list of columns.
        """
        return [
            'report_month',
            'bank_country_code',
            'business_mode',
            'brand',
            'size_ticket',
            'product_id',
            'range_program_id',
            'founding_source',
            'transaction_type',
            'jurisdiction',
            'reversal_indicator',
            'currency_local_indicator',
            'card_present_indicator',
            'account_number',
            'merchant_country_code',
            'switch_code',
            'transaction_count' ,
            'transaction_amount',
            'transaction_scheme_fee_cost',
            'unitary_scheme_fee_cost',
            'estimated_scheme_fee_amount',
            'unitary_estimated_scheme_fee_amount' 
        ]
    
    def get_report_legacy_columns(self)->list:
        """Return list of columns to insert/read  of report
        
        Returns:
            list: list of columns.
        """
        return [
            'rpt_bnk_id',
            'set_mth',
            'bus_id',
            'sch_id',
            'tkt_siz_id',
            'prd_id',
            'prg_id',
            'fnd_src_id',
            'txn_scp_id',
            'txn_typ_id',
            'txn_rvsl_flg_id',
            'txn_crncy_lcl_flg_id',
            'txn_crd_prs_flg_id',
            'mct_cd',
            'swt_cd',
            'txn_cnt',
            'txn_amt',
            'txn_sfc',
            'mct_ctry_id',
            'unt_sfc',
            'est_sch_fee_amt',
            'unt_est_sch_fee_amt'
        ]
    
    def get_report_legacy_columns_filter(self)->list:
        """Return list of columns to filter the report
        
        Returns:
            list: list of columns.
        """
        return [
            'app_id',
            'app_execution_id',
            'rpt_bnk_id',
            'set_mth',
            'bus_id',
            'sch_id',
            'tkt_siz_id',
            'prd_id',
            'prg_id',
            'fnd_src_id',
            'txn_scp_id',
            'txn_typ_id',
            'txn_rvsl_flg_id',
            'txn_crncy_lcl_flg_id',
            'txn_crd_prs_flg_id',
            'mct_cd',
            'swt_cd',
            'txn_cnt',
            'txn_amt',
            'txn_sfc',
            'mct_ctry_id',
            'unt_sfc',
            'est_sch_fee_amt',
            'unt_est_sch_fee_amt'
        ]

    def validation_conditions(self)->list:
        """Returns list of validation conditions
        
        Returns:
            list: list of conditions.
        """
        return [
            "report_amount is null or",
            "account_range_country is null or",
            "account_funding_source is null or",
            "account_number is null or",
            "jurisdiction is null or",
            "product_id is null or",
            "range_program_id is null",
        ] 
    
    def temp_table_scheme_fee_transaction(self)->str:
        """creates query for an empty clone transaction table to make all calculations
        
        Returns:
            query (str): generated query. 
        """
        # query = f"""select exchange_rate,report_amount,report_currency,{",".join(self.get_detail_columns())} 
        query = f"""select {",".join(self.get_detail_columns())} 
        from operational.mh_transaction_scheme_fee where app_id = -1 """
        return query

    def temp_table_scheme_fee_report(self)->str:
        """creates query for an empty clone report table to make all calculations
        
        Returns:
            str: generated query.
        """
        return f"""select app_id,
                    app_customer_code,
                    app_type_file,
                    app_hash_file,
                    app_processing_date, 
                    app_execution_id,
                    report_currency,
                    customer_code,
                    {",".join(self.get_report_columns())} from operational.mh_monthly_scheme_fee where app_id = -1 """

    def temp_table_scheme_fee_report_legacy(self)->str:
        """creates query for an empty clone report table to make all calculations
        
        Returns:
            str: generated query.
        """
        return f"""select app_id,
                          app_execution_id,
                          customer_code,
                          report_month,
                    {",".join(self.get_report_legacy_columns())} from operational.mh_monthly_scheme_fee_legacy where app_id = -1 """
    
    def get_insert_detail(self,columns:list,scheme:str,table:str)->str:
        """Returns basic insert query for detail table
        Args:
            columns (list): list of columns.
            schema (str): schema of table.
            table (str): table name.

        Returns:
            str: generated query.
        """
        return f"""
        SET ENABLE_PARTITIONWISE_JOIN TO ON;
        insert into {scheme}.{table} ({','.join(columns)})
        """
    
    def get_issuers_visa(self,formated_date:str,date:str,first_day:str,last_day:str,insert:str)->str:
        """Returns Query to get issuer transactions of visa
        
        Args:
            formated_date (str): formated date and execution time.
            date (str): year and month of report.
            first_date (str): start of range for report.
            last_day (str): end of range for report.
            insert  (str): insert query for report.

        Returns:
            str: returns query
        
        """
        client_code = str(self.client).lower()
        return f"""
            do
            $$
            declare
                query_date text;
                array_dates text[];
                partition_date text;
                query text; 
            begin
                array_dates := ARRAY(SELECT CONCAT(LEFT(DATE::TEXT,4),'-',SUBSTRING(DATE::TEXT,6,2),'-',SUBSTRING(DATE::TEXT,9,2))
                                                FROM GENERATE_SERIES('{first_day}'::DATE, '{last_day}'::DATE, '1 day') AS DATE);                
                        
                foreach query_date in array array_dates
                loop
                    partition_date  := CONCAT(LEFT(query_date,4),SUBSTRING(query_date,6,2),SUBSTRING(query_date,9,2));  
                    query :=' 
                    {insert}
                    select 
                        t.app_type_file,
                        cus.code,
                        ''{formated_date}'',
                        ''{date}'',
                        t.app_id,
                        t.app_hash_file,
                        ''VISA ISS'',
                        t.app_processing_date,
                        cast(t.account_number as text),
                        t.card_acceptor_id,
                        case when cf.funding_source in (''C'',''H'',''R'') then ''C'' when cf.funding_source in(''D'',''P'') then ''D'' else null end as account_funding_source,
                        ardef_country,
                        cf.product_id, 
                        sfbp.range_program_id,
                        jurisdiction,
                        btt.transaction_type_id,
                        reversal_indicator,
                        case when cur.currency_alphabetic_code = cus.local_currency_code or (dcc_indicator <> ''1'' or dcc_indicator is null) then 1 else 0 end,
                        case when cast(case when trim(motoec_indicator) = '''' then ''10'' else motoec_indicator end as integer) < 10 then 0 else 1 end,
                        1,
                        settlement_report_amount, 
                        0,
                        ''VISA'',
                        merchant_country_code,
                        '''' switch_code,
                        settlement_report_currency_code, 
                        cus.customer_country, 
                        cf.business_mode,
                        0,
                        0,
                        0,
                        0,
                        purchase_date,
                        coalesce(x1.exchange_value,1) ,
                        coalesce(x1.exchange_value,1) * t.source_amount ,
                        cus.report_currency_code
                    from 
                    operational.dh_visa_transaction_{client_code}_in t
                    inner join operational.dh_visa_transaction_calculated_field_{client_code}_in cf on t.app_id = cf.app_id and t.app_hash_file = cf.app_hash_file and t.app_processing_date = cf.app_processing_date
                    inner join control.t_customer cus on cus.code = t.app_customer_code 
                    left join operational.m_currency cur on t.source_currency_code = cast(cur.currency_numeric_code as text) 
                    left join operational.m_visa_business_transaction_type btt on cast(business_transaction_type_id as integer) = cf.business_transaction_type
                    left join operational.m_scheme_fee_bin_products sfbp on trim(sfbp.product_code) = trim(cf.product_id) and sfbp.brand = ''VISA''
                    left join operational.dh_exchange_rate x1 on x1.app_processing_date = t.app_processing_date and x1.brand = ''VISA'' and x1.currency_from_code::int = t.source_currency_code::int and x1.currency_to = cus.report_currency_code
                    where
                    t.app_processing_date ='''||query_date||'''
                    and cf.app_processing_date ='''||query_date||'''
                    and btt.transaction_type_id in (''PUR'', ''CRD'', ''CSH'')
                    and t.transaction_code in (''05'',''06'',''07'',''25'',''26'',''27'')
                    ;';
                        
                    EXECUTE query;
                end loop;
            end;
            $$;
        """
    
    def get_acquirer_visa(self,formated_date:str,date:str,first_day:str,last_day:str,insert:str)->str:
        """Returns query to get visa acquiring transactions
        
        Args:
            formated_date (str): formated date and execution time.
            date (str): year and month of report.
            first_date (str): start of range for report.
            last_day (str): end of range for report.
            insert  (str): insert query for report.

        Returns:
            str: returns query
        
        """
        client_code = str(self.client).lower()
        return f"""
            do
            $$
            declare
                query_date text;
                array_dates text[];
                partition_date text;
                query text; 
            begin
                array_dates := ARRAY(SELECT CONCAT(LEFT(DATE::TEXT,4),'-',SUBSTRING(DATE::TEXT,6,2),'-',SUBSTRING(DATE::TEXT,9,2))
                                                FROM GENERATE_SERIES('{first_day}'::DATE, '{last_day}'::DATE, '1 day') AS DATE);                
                        
                foreach query_date in array array_dates
                loop
                    partition_date  := CONCAT(LEFT(query_date,4),SUBSTRING(query_date,6,2),SUBSTRING(query_date,9,2));  
                    query :=' 
                    {insert}
                    select
                        t.app_type_file,
                        cus.code,
                        ''{formated_date}'',
                        ''{date}'',
                        t.app_id,
                        t.app_hash_file,
                        ''VISA ACQ'',
                        t.app_processing_date,
                        cast(t.account_number as text),
                        t.card_acceptor_id card_acceptor_id,
                        case when cf.funding_source in (''C'',''H'',''R'') then ''C'' when cf.funding_source in(''D'',''P'') then ''D'' else null end as account_funding_source,
                        ardef_country,
                        cf.product_id, 
                        sfbp.range_program_id,
                        jurisdiction,
                        btt.transaction_type_id,
                        reversal_indicator,
                        case when cur.currency_alphabetic_code = cus.local_currency_code or (dcc_indicator <> ''1'' or dcc_indicator is null) then 1 else 0 end,
                        case when cast(case when trim(motoec_indicator) = '''' then ''10'' else motoec_indicator end as integer) < 10 then 0 else 1 end,
                        1,
                        settlement_report_amount, 
                        0,
                        ''VISA'',
                        merchant_country_code,
                        '''' switch_code,
                        settlement_report_currency_code, 
                        cus.customer_country, 
                        cf.business_mode,
                        0,
                        0,
                        0,
                        0,
                        purchase_date,
                        coalesce(x1.exchange_value,1),
                        coalesce(x1.exchange_value,1) * t.source_amount,
                        cus.report_currency_code
                    from
                    operational.dh_visa_transaction_{client_code}_out t
                    inner join operational.dh_visa_transaction_calculated_field_{client_code}_out cf on t.app_id = cf.app_id and t.app_hash_file = cf.app_hash_file and t.app_processing_date = cf.app_processing_date    
                    inner join control.t_customer cus on cus.code = t.app_customer_code
                    left join operational.m_currency cur on t.source_currency_code = cast(cur.currency_numeric_code as text)
                    left join operational.m_visa_business_transaction_type btt on cast(business_transaction_type_id as integer) = cf.business_transaction_type
                    left join operational.m_scheme_fee_bin_products sfbp on trim(sfbp.product_code) = trim(cf.product_id) and sfbp.brand = ''VISA''     
                    left join operational.dh_exchange_rate x1 on x1.app_processing_date = t.app_processing_date and x1.brand = ''VISA'' and x1.currency_from_code::int = t.source_currency_code::int and x1.currency_to = cus.report_currency_code
                    where
                    t.app_processing_date ='''||query_date||'''
                    and cf.app_processing_date ='''||query_date||'''
                    and btt.transaction_type_id in (''PUR'', ''CRD'', ''CSH'')
                    and t.transaction_code in (''05'',''06'',''07'',''25'',''26'',''27'')
                    ;';                        
                    EXECUTE query;
                end loop;
            end;
            $$;
        """

    def get_on_us_visa(self,formated_date:str,date:str,first_day:str,last_day:str,insert:str)->str:
        """Returns query to get missing on us transactions (issuing on-us, because these are duplicates of acquiring on-us)
        
        Args:
            formated_date (str): formated date and execution time.
            date (str): year and month of report.
            first_date (str): start of range for report.
            last_day (str): end of range for report.
            insert  (str): insert query for report.

        Returns:
            str: returns query
        
        """
        client_code = str(self.client).lower()
        return f"""
            do
            $$
            declare
                query_date text;
                array_dates text[];
                partition_date text;
                query text; 
            begin
                array_dates := ARRAY(SELECT CONCAT(LEFT(DATE::TEXT,4),'-',SUBSTRING(DATE::TEXT,6,2),'-',SUBSTRING(DATE::TEXT,9,2))
                                                FROM GENERATE_SERIES('{first_day}'::DATE, '{last_day}'::DATE, '1 day') AS DATE);                
                        
                foreach query_date in array array_dates
                loop
                    partition_date  := CONCAT(LEFT(query_date,4),SUBSTRING(query_date,6,2),SUBSTRING(query_date,9,2));  
                    query :=' 
                    {insert}
                    select 
                        t.app_type_file,
                        cus.code,
                        ''{formated_date}'',
                        ''{date}'',
                        t.app_id,
                        t.app_hash_file,
                        ''VISA ON-US DUP (ACQ TO ISS)'',
                        t.app_processing_date,
                        cast(t.account_number as text),
                        t.card_acceptor_id card_acceptor_id,
                        case
                            when cf.funding_source in ( ''C'', ''H'', ''R'' ) then ''C''
                            when cf.funding_source in( ''D'', ''P'' ) then ''D''
                            else null
                        end                as account_funding_source,
                        ardef_country,
                        cf.product_id,
                        sfbp.range_program_id,
                        jurisdiction,
                        btt.transaction_type_id,
                        reversal_indicator,
                        case when cur.currency_alphabetic_code = cus.local_currency_code or (dcc_indicator <> ''1'' or dcc_indicator is null) then 1 else 0 end,
                        case
                            when Cast(case
                                        when Trim(motoec_indicator) = '''' then ''10''
                                        else motoec_indicator
                                    end as integer) < 10 then 0
                            else 1
                        end,
                        1,
                        settlement_report_amount,
                        0,
                        ''VISA'',
                        merchant_country_code,
                        '''' switch_code,
                        settlement_report_currency_code,
                        cus.customer_country,
                        ''issuing'',
                        0,
                        0,
                        0,
                        0,
                        purchase_date,
                        coalesce(x1.exchange_value,1),
                        coalesce(x1.exchange_value,1) * t.source_amount,
                        cus.report_currency_code
                    from operational.dh_visa_transaction_{client_code}_out t
                    inner join operational.dh_visa_transaction_calculated_field_{client_code}_out cf on t.app_id = cf.app_id and t.app_hash_file = cf.app_hash_file and t.app_processing_date = cf.app_processing_date
                    inner join control.t_customer cus on cus.code = t.app_customer_code
                    left join operational.m_currency cur on t.source_currency_code = Cast(cur.currency_numeric_code as TEXT)
                    left join operational.m_visa_business_transaction_type btt on Cast(business_transaction_type_id as integer) = cf.business_transaction_type
                    left join operational.m_scheme_fee_bin_products sfbp on Trim(sfbp.product_code) = Trim(cf.product_id) and sfbp.brand = ''VISA''
                    left join operational.dh_exchange_rate x1 on x1.app_processing_date = t.app_processing_date and x1.brand = ''VISA'' and x1.currency_from_code::int = t.source_currency_code::int and x1.currency_to = cus.report_currency_code
                    where t.app_processing_date ='''||query_date||'''
                    and cf.app_processing_date ='''||query_date||'''
                    and btt.transaction_type_id in ( ''PUR'', ''CRD'', ''CSH'' )
                    and t.transaction_code in ( ''05'', ''06'', ''07'', ''25'', ''26'', ''27'' )
                    and jurisdiction = ''on-us''
                    ;';                        
                    EXECUTE query;
                end loop;
            end;
            $$;
        """
    
    def get_sms_visa(self,formated_date:str,date:str,first_day:str,last_day:str,insert:str)->str:
        """Returns query to get sms visa transactions
        
        Args:
            formated_date (str): formated date and execution time.
            date (str): year and month of report.
            first_date (str):  start of range for report.
            last_day (str): end of range for report.
            insert  (str): insert query for report.

        Returns:
            str: returns query
        
        """
        client_code = str(self.client).lower()
        return f"""
            do
            $$
            declare
                query_date text;
                array_dates text[];
                partition_date text;
                query text; 
            begin
                array_dates := ARRAY(SELECT CONCAT(LEFT(DATE::TEXT,4),'-',SUBSTRING(DATE::TEXT,6,2),'-',SUBSTRING(DATE::TEXT,9,2))
                                                FROM GENERATE_SERIES('{first_day}'::DATE, '{last_day}'::DATE, '1 day') AS DATE);                
                        
                foreach query_date in array array_dates
                loop
                    partition_date  := CONCAT(LEFT(query_date,4),SUBSTRING(query_date,6,2),SUBSTRING(query_date,9,2));  
                    query :=' 
                    {insert}
                    select 
                        sms.app_type_file,
                        cus.code,
                        ''{formated_date}'',
                        ''{date}'',
                        sms.app_id,
                        sms.app_hash_file,
                        ''VISA SMS'',
                        sms.app_processing_date,
                        cast(sms.card_number as text),
                        sms.card_acceptor_terminal_id card_acceptor_id,
                        case when sms.account_funding_source in (''C'',''H'',''R'') then ''C'' when sms.account_funding_source in(''D'',''P'') then ''D'' else null end,
                        cast(pan_extended_country_code as text),
                        sms.product_id_sms,
                        sfbp.range_program_id, 
                        jurisdiction,
                        btt.transaction_type_id,
                        reversal_indicator,
                        case when transaction_currency_code = cus.local_currency_code or (dcc_indicator_sms <> ''1'' or dcc_indicator_sms is null) then 1 else 0 END,
                        case when cast(case when trim(sms.mailtelephone_or_electronic_commerce_indicator) = '''' then ''10'' else sms.mailtelephone_or_electronic_commerce_indicator end as integer) < 10 then 0 else 1 end,
                        1,
                        settlement_amount,
                        0,
                        ''VISA'',
                        card_acceptor_country,
                        '''' switch_code,
                        c.currency_alphabetic_code,
                        cus.customer_country,
                        cf.business_mode,
                        0,
                        0,
                        0,
                        0,
                        local_transaction_date,
                        case when sms.transaction_amount = 0 then coalesce(x2.exchange_value, 1) else coalesce(x1.exchange_value, 1) end,
                        case when sms.transaction_amount = 0 then coalesce(x2.exchange_value, 1) * (sms.cryptogram_amount + sms.surcharge_amount_sms) else coalesce(x1.exchange_value, 1) * sms.transaction_amount end,
                        cus.report_currency_code
                    from operational.dh_visa_transaction_sms_{client_code}_in sms
                    inner join operational.dh_visa_transaction_sms_calculated_field_{client_code}_in cf
                    on sms.app_id = cf.app_id and sms.app_hash_file = cf.app_hash_file and sms.app_processing_date = cf.app_processing_date
                    inner join control.t_customer cus on cus.code = sms.app_customer_code
                    left join operational.m_currency c on sms.settlement_currency_code_sms = cast(c.currency_numeric_code as text)
                    left join operational.m_scheme_fee_bin_products sfbp on trim(sfbp.product_code) = trim(sms.product_id_sms) and sfbp.brand = ''VISA'' 
                    left join operational.m_visa_business_transaction_type btt on cast(business_transaction_type_id as integer) = cf.business_transaction_type
                    left join operational.dh_exchange_rate x1 on x1.app_processing_date = sms.app_processing_date and x1.brand = ''VISA'' and x1.currency_from_code::int = sms.transaction_currency_code::int and x1.currency_to = cus.report_currency_code
                    left join operational.dh_exchange_rate x2 on x2.app_processing_date = sms.app_processing_date and x2.brand = ''VISA'' and x2.currency_from_code::int = sms.cryptogram_currency_code::int and x2.currency_to = cus.report_currency_code 
                    where
                    sms.app_processing_date ='''||query_date||'''
                    and cf.app_processing_date ='''||query_date||'''
                    and btt.transaction_type_id in (''PUR'', ''CRD'', ''CSH'')
                    and cf.transaction_code_sms in (''05'',''06'',''07'',''25'',''26'',''27'')
                    ;';                        
                    EXECUTE query;
                end loop;
            end;
            $$;
        """
    
    def get_sms_on_us_visa(self,formated_date:str,date:str,first_day:str,last_day:str,insert:str)->str:
        """Returns query to get missing on us transactions (issuing on-us, because these are duplicates of acquiring on-us)
        
        Args:
            formated_date (str): formated date and execution time.
            date (str): year and month of report.
            first_date (str): start of range for report.
            last_day (str): end of range for report.
            insert  (str): insert query for report.

        Returns:
            str: returns query
        
        """
        client_code = str(self.client).lower()
        return f"""
            do
            $$
            declare
                query_date text;
                array_dates text[];
                partition_date text;
                query text; 
            begin
                array_dates := ARRAY(SELECT CONCAT(LEFT(DATE::TEXT,4),'-',SUBSTRING(DATE::TEXT,6,2),'-',SUBSTRING(DATE::TEXT,9,2))
                                                FROM GENERATE_SERIES('{first_day}'::DATE, '{last_day}'::DATE, '1 day') AS DATE);                
                        
                foreach query_date in array array_dates
                loop
                    partition_date  := CONCAT(LEFT(query_date,4),SUBSTRING(query_date,6,2),SUBSTRING(query_date,9,2));  
                    query :=' 
                    {insert}
                    select
                        sms.app_type_file,
                        cus.code,
                        ''{formated_date}'',
                        ''{date}'',
                        sms.app_id,
                        sms.app_hash_file,
                        ''VISA ON-US DUP (SMS TO ISS)'',
                        sms.app_processing_date,
                        Cast(sms.card_number as TEXT),
                        sms.card_acceptor_terminal_id card_acceptor_id,
                        case
                            when sms.account_funding_source in ( ''C'', ''H'', ''R'' ) then ''C''
                            when sms.account_funding_source in( ''D'', ''P'' ) then ''D''
                            else null
                        end,
                        Cast(pan_extended_country_code as TEXT),
                        sms.product_id_sms,
                        sfbp.range_program_id,
                        jurisdiction,
                        btt.transaction_type_id,
                        reversal_indicator,
                        case when transaction_currency_code = cus.local_currency_code or (dcc_indicator_sms <> ''1'' or dcc_indicator_sms is null) then 1 else 0 END,
                        case
                            when Cast(case
                            when Trim(sms.mailtelephone_or_electronic_commerce_indicator) = ''''
                                    then
                                        ''10''
                                        else sms.mailtelephone_or_electronic_commerce_indicator
                                    end as integer) < 10 then 0
                            else 1
                        end,
                        1,
                        settlement_amount,
                        0,
                        ''VISA'',
                        card_acceptor_country,
                        '''' switch_code,
                        currency_alphabetic_code,
                        cus.customer_country,
                        ''issuing'',
                        0,
                        0,
                        0,
                        0,
                        local_transaction_date,
                        case when sms.transaction_amount = 0 then coalesce(x2.exchange_value, 1) else coalesce(x1.exchange_value, 1) end,
                        case when sms.transaction_amount = 0 then coalesce(x2.exchange_value, 1) * (sms.cryptogram_amount + sms.surcharge_amount_sms) else coalesce(x1.exchange_value, 1) * sms.transaction_amount end,
                        cus.report_currency_code
                    from operational.dh_visa_transaction_sms_{client_code}_in sms
                    inner join operational.dh_visa_transaction_sms_calculated_field_{client_code}_in cf on sms.app_id = cf.app_id and sms.app_hash_file = cf.app_hash_file and sms.app_processing_date = cf.app_processing_date
                    inner join control.t_customer cus on cus.code = sms.app_customer_code
                    left join operational.m_currency cur on sms.settlement_currency_code_sms = Cast(cur.currency_numeric_code as TEXT)
                    left join operational.m_scheme_fee_bin_products sfbp on Trim(sfbp.product_code) = Trim(sms.product_id_sms) and sfbp.brand = ''VISA''
                    left join operational.m_visa_business_transaction_type btt on Cast(business_transaction_type_id as integer) = cf.business_transaction_type
                    left join operational.dh_exchange_rate x1 on x1.app_processing_date = sms.app_processing_date and x1.brand = ''VISA'' and x1.currency_from_code::int = sms.transaction_currency_code::int and x1.currency_to = cus.report_currency_code
                    left join operational.dh_exchange_rate x2 on x2.app_processing_date = sms.app_processing_date and x2.brand = ''VISA'' and x2.currency_from_code::int = sms.cryptogram_currency_code::int and x2.currency_to = cus.report_currency_code 
                    where sms.app_processing_date ='''||query_date||'''
                    and cf.app_processing_date ='''||query_date||'''
                    and btt.transaction_type_id in ( ''PUR'', ''CRD'', ''CSH'' )
                    and cf.transaction_code_sms in ( ''05'', ''06'', ''07'', ''25'', ''26'', ''27'' )
                    and issuer_acquirer_indicator = ''A''
                    and jurisdiction = ''on-us''
                    ;';                        
                    EXECUTE query;
                end loop;
            end;
            $$;
        """

    def get_transactions_mastercard(self,formated_date:str,date:str,first_day:str,last_day:str,insert:str)->str:
        """Returns query to get mastercard transactions in general
        
        Args:
            formated_date (str): formated date and execution time.
            date (str): year and month of report.
            first_date (str): start of range for report.
            last_day (str): end of range for report.
            insert  (str): insert query for report.

        Returns:
            str: returns query
        
        """
        client_code = str(self.client).lower()
        return f"""
            do
            $$
            declare
                query_date text;
                array_dates text[];
                partition_date text;
                query text; 
            begin
                array_dates := ARRAY(SELECT CONCAT(LEFT(DATE::TEXT,4),'-',SUBSTRING(DATE::TEXT,6,2),'-',SUBSTRING(DATE::TEXT,9,2))
                                                FROM GENERATE_SERIES('{first_day}'::DATE, '{last_day}'::DATE, '1 day') AS DATE);                
                        
                foreach query_date in array array_dates
                loop
                    partition_date  := CONCAT(LEFT(query_date,4),SUBSTRING(query_date,6,2),SUBSTRING(query_date,9,2));  
                    query :=' 
                    {insert}
                    select 
                        mde.app_type_file,
                        cus.code,
                        ''{formated_date}'',
                        ''{date}'',
                        mde.app_id,
                        mde.app_hash_file,
                        ''MASTERCARD ISS AND ACQ'',
                        mde.app_processing_date,
                        left(cast(pan as text), 16) as account_number,
                        cast(card_acceptor_id_code as text),
                        cf.funding_source,
                        iar_country,
                        cf.gcms_product_identifier, 
                        sfbp.range_program_id,
                        cf.jurisdiction, 
                        mcbtt.transaction_type_id,
                        case when (app_message_type = ''1240'' and Left(message_reversal_indicator, 1) = ''R'') or (app_message_type = ''1442'' and Left(message_reversal_indicator,1) <> ''R'') then 1 else 0 end, 
                        case when cur.currency_alphabetic_code in (cus.local_currency_code) then 1 else 0 end,
                        case when substring(pos_entry_mode, 6, 1) = ''0'' then 0 else 1 end as motoec_indicator,
                        1,
                        settlement_report_amount,
                        0,
                        ''MasterCard'',
                        mde.card_acceptor_country_code,
                        '''' switch_code,
                        settlement_report_currency_code,
                        cus.customer_country,
                        business_mode,
                        0,
                        0,
                        0,
                        0,
                        date_and_time_local_transaction::date,
                        coalesce(x1.exchange_value, 1),
                        coalesce(x1.exchange_value, 1) * mde.amount_transaction,
                        cus.report_currency_code
                    from operational.dh_mastercard_data_element_{client_code} mde
                    inner join operational.dh_mastercard_calculated_field_{client_code} cf on  mde.app_id = cf.app_id and mde.app_hash_file = cf.app_hash_file and mde.app_processing_date = cf.app_processing_date and mde.app_type_file = cf.app_type_file
                    inner join control.t_customer cus on cus.code = mde.app_customer_code
                    left join operational.m_scheme_fee_bin_products sfbp on trim(sfbp.product_code) = trim(cf.gcms_product_identifier) and sfbp.brand = ''MC''
                    left join operational.m_bin_funding_source lubfs on rng_fnd_id = bin_funding_source_mc_numeric_code
                    left join operational.m_mastercard_business_transaction_type mcbtt on business_transaction_type_id = left(processing_code,2) 
                    left join operational.m_currency cur on mde.currency_code_transaction = cur.currency_numeric_code
                    left join operational.dh_exchange_rate x1 on x1.app_processing_date = mde.app_processing_date and x1.brand = ''MasterCard'' and x1.currency_from_code = mde.currency_code_transaction::text and x1.currency_to = cus.report_currency_code
                    where 
                    (mde.app_type_file = ''IN'' or mde.app_type_file = ''OUT'')
                    and (cf.app_type_file = ''IN'' or cf.app_type_file = ''OUT'')
                    and mde.app_processing_date ='''||query_date||'''
                    and cf.app_processing_date ='''||query_date||'''
                    and app_message_type in (''1240'',''1442'')
                    ;';                        
                    EXECUTE query;
                end loop;
            end;
            $$;
        """

    def get_on_us_mastercard(self,formated_date:str,date:str,first_day:str,last_day:str,insert:str)->str:
        """Returns query to get mastercard missing on us transactions (issuing on-us, because these are duplicates of acquiring on-us)
        
        Args:
            formated_date (str): formated date and execution time.
            date (str): year and month of report.
            first_date (str): start of range for report.
            last_day (str): end of range for report.
            insert  (str): insert query for report.

        Returns:
            str: returns query
        
        """
        client_code = str(self.client).lower()
        return f"""
            do
            $$
            declare
                query_date text;
                array_dates text[];
                partition_date text;
                query text; 
            begin
                array_dates := ARRAY(SELECT CONCAT(LEFT(DATE::TEXT,4),'-',SUBSTRING(DATE::TEXT,6,2),'-',SUBSTRING(DATE::TEXT,9,2))
                                                FROM GENERATE_SERIES('{first_day}'::DATE, '{last_day}'::DATE, '1 day') AS DATE);                
                        
                foreach query_date in array array_dates
                loop
                    partition_date  := CONCAT(LEFT(query_date,4),SUBSTRING(query_date,6,2),SUBSTRING(query_date,9,2));  
                    query :=' 
                    {insert}
                    select
                        mde.app_type_file,
                        cus.code,
                        ''{formated_date}'',
                        ''{date}'',
                        mde.app_id,
                        mde.app_hash_file,
                        ''MASTERCARD ON-US DUP (ACQ TO ISS)'',
                        mde.app_processing_date,
                        left(cast(pan as text), 16) as account_number,
                        cast(card_acceptor_id_code as text),
                        cf.funding_source,
                        iar_country,
                        cf.gcms_product_identifier,
                        sfbp.range_program_id,
                        cf.jurisdiction,
                        mcbtt.transaction_type_id,
                        case
                            when (app_message_type = ''1240'' and Left(message_reversal_indicator, 1) = ''R'') or (app_message_type = ''1442'' and Left(message_reversal_indicator,1) <> ''R'') then 1
                            else 0
                        end,
                        case
                            when cur.currency_alphabetic_code in (
                                cus.local_currency_code ) then 1
                            else 0
                        end,
                        case
                            when Substring(pos_entry_mode, 6, 1) = ''0'' then 0
                            else 1
                        end as motoec_indicator,
                        1,
                        settlement_report_amount,
                        0,
                        ''MasterCard'',
                        mde.card_acceptor_country_code,
                        '''' switch_code,
                        settlement_report_currency_code,
                        cus.customer_country,
                        ''issuing'',
                        0,
                        0,
                        0,
                        0,
                        date_and_time_local_transaction :: date,
                        coalesce(x1.exchange_value, 1),
                        coalesce(x1.exchange_value, 1) * mde.amount_transaction,
                        cus.report_currency_code
                    from operational.dh_mastercard_data_element_{client_code}_out mde
                    inner join operational.dh_mastercard_calculated_field_{client_code}_out cf on mde.app_id = cf.app_id and mde.app_hash_file = cf.app_hash_file and mde.app_processing_date = cf.app_processing_date
                    inner join control.t_customer cus on cus.code = mde.app_customer_code
                    left join operational.m_scheme_fee_bin_products sfbp on Trim(sfbp.product_code) = Trim(cf.gcms_product_identifier) and sfbp.brand = ''MC''
                    left join operational.m_bin_funding_source lubfs on rng_fnd_id = bin_funding_source_mc_numeric_code
                    left join operational.m_mastercard_business_transaction_type mcbtt on business_transaction_type_id = Left(processing_code, 2)
                    left join operational.m_currency cur on mde.currency_code_transaction = cur.currency_numeric_code
                    left join operational.dh_exchange_rate x1 on x1.app_processing_date = mde.app_processing_date and x1.brand = ''MasterCard'' and x1.currency_from_code = mde.currency_code_transaction::text and x1.currency_to = cus.report_currency_code
                    where mde.app_processing_date ='''||query_date||'''
                    and cf.app_processing_date ='''||query_date||'''
                    and app_message_type in ( ''1240'', ''1442'' )
                    and cf.jurisdiction = ''on-us''
                    ;';                        
                    EXECUTE query;
                end loop;
            end;
            $$;
        """
    
    def get_union_query(self,base:str,other_query:list)->str:
        """Returns combination in union query
        
         Args:
            base (str): first part of query.
            other_query (list): list of querys to unify.

        Returns:
            str: returns query
        
        """
        joined = " union all".join(other_query)
        return f"""{base} {joined}"""
        
    def get_exchange_rate_calculation(self,schema:str,table_name:str,first_day:str,last_day:str)->str:
        """Return Query for exchange rate calculations
        
        Args:
            schema (str): schema of table.
            table_name (str): name of table.
            first_date (str): start of range for report.
            last_day (str): end of range for report.

        Returns:
            str: returns query

        """
        return f"""
            SET ENABLE_PARTITIONWISE_JOIN TO ON;
            update {schema}.{table_name} d set exchange_rate = x.exchange_value,
            report_amount = round(case 
                            when x.settlement_currency = x.report_currency_code then x.settlement_amount 
                                else x.settlement_amount * x.exchange_value 
                            end::numeric,2) ,
            report_currency = x.report_currency_code
            from (
                select
                    case when settlement_currency = cus.report_currency_code then 1 else mc.exchange_value end exchange_value,
                    cus.report_currency_code,
                    tsf.app_id,
                    tsf.app_hash_file,
                    settlement_currency,
                    settlement_amount
                from {schema}.{table_name} tsf
                inner join control.t_customer cus on cus.code = tsf.app_customer_code
                left join operational.dh_exchange_rate mc on tsf.app_processing_date = mc.app_processing_date
                        and tsf.transaction_brand = mc.brand
                        and mc.currency_to = cus.report_currency_code
                        and mc.currency_from = tsf.settlement_currency
                where 
                (tsf.app_processing_date between '{first_day}' and '{last_day}')
                and tsf.app_customer_code = '{self.client}') x 
            where x.app_id = d.app_id and x.app_hash_file = d.app_hash_file
        """

    def get_rows_w_null_conditions(self,first_day:str,last_day:str,formated_date:str)->str:
        """gets rows with null fields

        Args:
            first_date (str): start of range for report.
            last_day (str): end of range for report.
            formated_date (str): formated date of execution.

        Returns:
            str: returns query 
        
        """
        return f"""
            where app_processing_date between '{first_day}' and '{last_day}'
            and app_customer_code = '{self.client}'
            and (
                {" ".join(self.validation_conditions())}
            )
            and app_execution_id = '{formated_date}'
            """

    def delete_rows_w_null(self,schema:str,table:str,conditions:str)->str:
        """Deletes rows with null fields
        
        Args:
            schema (str): schema of table.
            table (str): table name.
            conditions (str): conditions query.

        Returns:
            str: returns query 
        
        """
        return f"""delete from {schema}.{table} {conditions}"""

    def update_amounts(self,first_day:str,last_day:str)->str:
        """Update null amounts
        
        Args:
            first_date (str): start of range for report.
            last_day (str): end of range for report.

        Returns:
            str: returns query
        
        """
        return  f"""
            update operational.mh_transaction_scheme_fee d set
            report_amount = 0
            where app_processing_date between '{first_day}' and '{last_day}'
            and app_customer_code = '{self.client}' and report_amount is null
        """ 

    def update_size_tickets(self,schema:str,table_name:str,first_date:str, last_day:str )->str:
        """updates size tikets field
        
        Args:
            schema (str): schema of table.
            table_name (str): table name.
            first_date (str): start of range for report.
            last_day (str): end of range for report.

        Returns:
            str: returns query
        
        """
        return f"""
            do
            $$
            declare
                query_date text;
                array_dates text[];
                partition_date text;
                query text; 
            begin
                array_dates := ARRAY(SELECT CONCAT(LEFT(DATE::TEXT,4),'-',SUBSTRING(DATE::TEXT,6,2),'-',SUBSTRING(DATE::TEXT,9,2))
                                                FROM GENERATE_SERIES('{first_date}'::DATE, '{last_day}'::DATE, '1 day') AS DATE); 
                        
                foreach query_date in array array_dates
                loop
                    partition_date  := CONCAT(LEFT(query_date,4),SUBSTRING(query_date,6,2),SUBSTRING(query_date,9,2));  
                    query :=' 
                    with size_ticket_cte as (
                    select tsf.app_id,
                            tsf.app_hash_file,
                            tsf.app_processing_date,
                            case
                                when tsf.report_currency = st.ticket_currency then tsf.report_amount
                                else tsf.report_amount * ex.exchange_value end ticket_amount,
                            st.app_id                                          size_ticket
                    from {schema}.{table_name}_'||partition_date||' tsf
                    left join (select distinct country_code_list, brand, ticket_currency
                                        from operational.m_size_ticket pre) as pre
                                        on tsf.transaction_brand = pre.brand
                                            and tsf.bank_country = pre.country_code_list
                            left join operational.dh_exchange_rate_'||partition_date||' ex
                                        on ex.app_processing_date = tsf.app_processing_date
                                            and ex.brand = tsf.transaction_brand
                                            and ex.currency_from = tsf.report_currency
                                            and ex.currency_to = pre.ticket_currency
                            left join operational.m_size_ticket st on tsf.transaction_brand = st.brand and
                                                                        (case
                                                                            when tsf.report_currency = st.ticket_currency
                                                                                then tsf.report_amount
                                                                            else tsf.report_amount * ex.exchange_value end >=
                                                                        st.size_ticket_min and
                                                                        case
                                                                            when tsf.report_currency = st.ticket_currency
                                                                                then tsf.report_amount
                                                                            else tsf.report_amount * ex.exchange_value end <
                                                                        st.size_ticket_max) and
                                                                        tsf.bank_country = st.country_code_list
                    )
                    update {schema}.{table_name}_'||partition_date||' d 
                    SET size_ticket = COALESCE(x.size_ticket, 0)
                    FROM size_ticket_cte x
                    WHERE d.app_id = x.app_id 
                    and d.app_hash_file = x.app_hash_file
                    ;';
                        
                    EXECUTE query;
                end loop;
            end;
            $$;
        """
    
    def update_switch_codes(self,schema:str,table_name:str,first_date:str, last_day:str)->str:
        """updates switch code field
        
        Args:
            schema (str): schema of table.
            table_name (str): table name.

        Returns:
            str: returns query
        
        """
        return f"""
            do
            $$
            declare
                query_date text;
                array_dates text[];
                partition_date text;
                query text; 
            begin
                array_dates := ARRAY(SELECT CONCAT(LEFT(DATE::TEXT,4),'-',SUBSTRING(DATE::TEXT,6,2),'-',SUBSTRING(DATE::TEXT,9,2))
                                                FROM GENERATE_SERIES('{first_date}'::DATE, '{last_day}'::DATE, '1 day') AS DATE); 
                        
                foreach query_date in array array_dates
                loop
                    partition_date  := CONCAT(LEFT(query_date,4),SUBSTRING(query_date,6,2),SUBSTRING(query_date,9,2));  
                    query :=' 
                        UPDATE {schema}.{table_name}_'||partition_date||' d
                        SET switch_code = tbr.switch_code
                        FROM operational.m_bin_ranges tbr
                        WHERE d.account_number::numeric BETWEEN tbr.start_value::numeric AND COALESCE(tbr.end_value::numeric, tbr.start_value::numeric)
                        AND tbr.brand = d.transaction_brand
                        AND tbr.customer_code = d.app_customer_code ;
                    ;';
                        
                    EXECUTE query;
                end loop;
            end;
            $$;
        """
    
    def update_unknown_range_country(self,formated_date:str)->str:
        """updates account_range_country

        Args:
            formated_date (str): date of execution of file.

        Returns:
            str: returns query
        """
        return f"""
            update operational.mh_transaction_scheme_fee d set 
            account_range_country = 'UNK'
            where app_execution_id = '{formated_date}' and account_range_country is null
            and app_customer_code = '{self.client}' 
        """

    def update_unknown_founding_source(self,formated_date:str)->str:
        """updates account_founding_source
        
        Args:
            formated_date (str): date of execution of file.

        Returns:
            str: returns query
        """
        return f"""
            update operational.mh_transaction_scheme_fee d set 
            account_funding_source = 'UNK'
            where app_execution_id = '{formated_date}' and account_funding_source is null
            and app_customer_code = '{self.client}' 
        """

    def update_unknown_account_range(self,formated_date:str)->str:
        """updates account_number
        
        Args:
            formated_date (str): date of execution of file.

        Returns:
            str: returns query
        """
        return f"""
            update operational.mh_transaction_scheme_fee d set 
            account_number = '9999999999999999'
            where app_execution_id = '{formated_date}' and account_number is null
            and app_customer_code = '{self.client}' 
        """

    def update_jurisdiction(self,formated_date:str)->str:
        """updates juridiction
        
        Args:
            formated_date (str): date of execution of file.

        Returns:
            str: returns query
        """
        return f"""
            update operational.mh_transaction_scheme_fee d set 
            jurisdiction = 'UNK'
            where app_execution_id = '{formated_date}' and jurisdiction is null
            and app_customer_code = '{self.client}' and report_amount is null
        """

    def update_visa_unk_product(self,formated_date:str)->str:
        """updates the unknown visa products with dummy
        
        Args:
            formated_date (str): date of execution of file.

        Returns:
            str: returns query
        """
        return f"""
            update operational.mh_transaction_scheme_fee d set 
            product_id = 'UNK',range_program_id = 'UNK'
            where app_execution_id = '{formated_date}' and product_id is null
            and transaction_brand = 'VISA'
            and app_customer_code = '{self.client}' and report_amount is null
        """
    
    def update_mastercard_unk_product(self,formated_date:str)->str:
        """updates the unknown mastercard products with dummy
        
        Args:
            formated_date (str): date of execution of file.

        Returns:
            str: returns query
        """
        return f"""
            update operational.mh_transaction_scheme_fee d set 
            product_id = 'UNK',range_program_id = 'UNK'
            where app_execution_id = '{formated_date}' and product_id is null
            and transaction_brand = 'MasterCard' 
            and app_customer_code = '{self.client}' and report_amount is null
        """

    def update_unk_range_program(self,formated_date:str)->str:
        """updates unknown range program id
        
        Args:
            formated_date (str): date of execution of file.

        Returns:
            str: returns query
        """
        return f"""
            update operational.mh_transaction_scheme_fee d set 
            range_program_id = 'UNK'
            where app_execution_id = '{formated_date}' and range_program_id is null
            and app_customer_code = '{self.client}' 
        """

    def get_insert_into_report_table(self,report_scheme:str,report_table:str,transaction_scheme:str,transaction_table:str,formated_date:str,month_name:str,first_day:str,last_day:str)->str:
        """Returns insertion query for report
        
        Args:
            report_scheme (str): report table schema.
            report_table (str): report table name.
            transaction_scheme (str): transaction table schema.
            transaction_table (str): transaction table name.
            formated_date (str): date of execution of file.
            month_name (str): processing month.
            first_date (str): start of range for report.
            last_day (str): end of range for report.

        Returns:
            str: returns query
        
        """
        return f"""
        insert into {report_scheme}.{report_table} (
            app_execution_id,
            report_month,
            report_currency,
            customer_code,
            bank_country_code,
            business_mode,
            brand,
            size_ticket,
            product_id,
            range_program_id,
            founding_source,
            transaction_type,
            jurisdiction,
            reversal_indicator,
            currency_local_indicator,
            card_present_indicator, 
            account_number,
            merchant_country_code,
            switch_code,
            transaction_count,
            transaction_amount,
            transaction_scheme_fee_cost
        )
        select
        '{formated_date}' app_execution_id,'{month_name}' report_month,cus.report_currency_code,cus.code,bank_country,business_mode_id,transaction_brand,size_ticket,product_id,range_program_id,account_funding_source,
        business_transaction_type_id,jurisdiction,reversal_indicator,currency_local_indicator,motoec_indicator,account_number,
        merchant_country_code, switch_code, sum(transaction_count),sum(report_amount),0
        from {transaction_scheme}.{transaction_table} tsf
        inner join control.t_customer cus on cus.code = tsf.app_customer_code
        where tsf.app_processing_date between  '{first_day}' and '{last_day}'
        and tsf.app_customer_code = '{self.client}' 
        group by 
        app_execution_id,
        cus.report_currency_code,
        cus.code,bank_country,
        report_month,
        business_mode_id,
        transaction_brand,
        size_ticket,
        product_id,
        range_program_id,
        account_funding_source,
        business_transaction_type_id,
        jurisdiction,
        reversal_indicator,
        currency_local_indicator,
        motoec_indicator,
        account_number,
        merchant_country_code,
        switch_code
        """

    def get_insert_into_report_legacy_table(self,report_scheme:str,report_legacy_table:str,transaction_scheme:str,report_table:str,formated_date:str,month_name:str,first_day:str,last_day:str)->str:
        """Returns insertion query for report
        
        Args:
            report_scheme (str): report table schema.
            report_legacy_table (str): report legacy table name.
            transaction_scheme (str): transaction table schema.
            report_table (str): report table name.
            formated_date (str): date of execution of file.
            month_name (str): processing month.
            first_date (str): start of range for report.
            last_day (str): end of range for report.

        Returns:
            str: returns query
        
        """
        return f"""
        insert into {report_scheme}.{report_legacy_table} (
            app_id,
            app_execution_id,
            customer_code,
            report_month,
            RPT_BNK_ID,
            SET_MTH,
            BUS_ID,
            SCH_ID,
            TKT_SIZ_ID,
            PRD_ID,
            PRG_ID,
            FND_SRC_ID,
            TXN_SCP_ID,
            TXN_TYP_ID,
            TXN_RVSL_FLG_ID,
            TXN_CRNCY_LCL_FLG_ID,
            TXN_CRD_PRS_FLG_ID, 
            MCT_CD,
            SWT_CD,
            TXN_CNT,
            TXN_AMT,
            TXN_SFC,
            MCT_CTRY_ID,
            UNT_SFC,
            EST_SCH_FEE_AMT,
            UNT_EST_SCH_FEE_AMT

        )
        select
        sf.app_id app_id,
        sf.app_execution_id app_execution_id,
        sf.customer_code customer_code,
        sf.report_month report_month,
        sf.app_customer_code RPT_BNK_ID,
        sf.report_month::int SET_MTH,
        case sf.business_mode
            when 'acquiring' then 1
            when 'issuing' then 2
        end::smallint BUS_ID,
        case sf.brand
            when 'MasterCard' then 1
            when 'VISA' then 2
        end::smallint SCH_ID,
        sf.size_ticket::int TKT_SIZ_ID,
        sfp.legacy_product_id::int PRD_ID,
        sf.range_program_id::int PRG_ID,
        bfs.bin_funding_source_mc_numeric_code::int FND_SRC_ID,
        case
            when sf.brand = 'MasterCard' then
                case sf.jurisdiction
                    when 'on-us' then 1
                    when 'off-us' then 2
                    when 'intraregional' then 4
                    when 'interregional' then 8
                end
            
            when sf.brand = 'VISA' then
                case sf.jurisdiction
                    when 'on-us' then 1
                    when 'off-us' then 2
                    when 'intraregional' then 3
                    when 'interregional' then 4
                end
        end::int TXN_SCP_ID,
        case sf.transaction_type
            when 'PUR' then 1
            when 'CSH' then 8
            when 'CRD' then 1
        end::int TXN_TYP_ID,
        sf.reversal_indicator::smallint TXN_RVSL_FLG_ID,
        sf.currency_local_indicator::smallint  TXN_CRNCY_LCL_FLG_ID,
        sf.card_present_indicator::smallint TXN_CRD_PRS_FLG_ID,
        left(sf.account_number::text,16) MCT_CD,
        sf.switch_code,
        sf.transaction_count::int TXN_CNT,
        sf.transaction_amount::numeric TXN_AMT,
        sf.transaction_scheme_fee_cost::numeric TXN_SFC,
        case sf.brand
            when 'MasterCard' then c.legacy_country_id
            when 'VISA' then c1.legacy_country_id
        end::int MCT_CTRY_ID,
        unitary_scheme_fee_cost::numeric UNT_SFC,
        estimated_scheme_fee_amount::numeric EST_SCH_FEE_AMT,
        unitary_estimated_scheme_fee_amount::numeric UNT_EST_SCH_FEE_AMT
        from {transaction_scheme}.{report_table} sf
        left join operational.m_scheme_fee_bin_products sfp on trim(sf.product_id) = trim(sfp.product_code)
        left join operational.m_bin_funding_source bfs on sf.founding_source = bfs.bin_funding_source_code
        left join operational.m_country c on (sf.brand = 'MasterCard' and sf.merchant_country_code = c.country_code_alternative)
        left join operational.m_country c1 on (sf.brand = 'VISA' and sf.merchant_country_code = c1.country_code)
        """
    
    def get_report_extra_columns(self,schema:str,table:str,hash:str,first_day:str)->str:
        """get columns not included in report columns from report table

        Args:
            scheme (str):  table schema.
            table (str):  table name.
            hash (str): hash code of file.
            first_day (str): first day of month.

        Returns:
            str: returns query
        """
        return f"""
                update {schema}.{table} t 
                    set app_id = s.row_num,
                    app_customer_code = '{self.client}',
                    app_type_file = 'report',
                    app_hash_file='{hash}',
                    app_processing_date='{first_day}',
                    transaction_scheme_fee_cost = 0,
                    unitary_scheme_fee_cost = 0,
                    estimated_scheme_fee_amount = 0,
                    unitary_estimated_scheme_fee_amount = 0 
                from (select st.*, row_number() over (order by account_number,
                        bank_country_code,
                        business_mode,
                        brand,
                        jurisdiction,
                        founding_source,
                        product_id,
                        range_program_id,
                        transaction_type,
                        reversal_indicator,
                        card_present_indicator,
                        currency_local_indicator,
                        merchant_country_code,
                        size_ticket) as row_num from {schema}.{table} st ) s 
                where 
                coalesce(s.account_number, '') = coalesce(t.account_number, '') and
                coalesce(s.bank_country_code, '') = coalesce(t.bank_country_code, '') and
                coalesce(s.business_mode, '') = coalesce(t.business_mode, '') and
                coalesce(s.brand, '') = coalesce(t.brand, '') and
                coalesce(s.jurisdiction, '') = coalesce(t.jurisdiction, '') and
                coalesce(s.founding_source, '') = coalesce(t.founding_source, '') and
                coalesce(s.product_id, '') = coalesce(t.product_id, '') and
                coalesce(s.range_program_id, '') = coalesce(t.range_program_id, '') and
                coalesce(s.transaction_type, '') = coalesce(t.transaction_type, '') and
                coalesce(s.reversal_indicator, '') = coalesce(t.reversal_indicator, '') and
                coalesce(s.card_present_indicator, '') = coalesce(t.card_present_indicator, '') and
                coalesce(s.currency_local_indicator, '') = coalesce(t.currency_local_indicator, '') and
                coalesce(s.merchant_country_code, '') = coalesce(t.merchant_country_code, '') and
                coalesce(s.size_ticket, '') = coalesce(t.size_ticket, '')
                """
    
    def get_report_legacy_query(self,schema:str,table:str,report_columns:list,formated_date:str)->str:
        """Returns query to get report

        Args:
            scheme (str):  table schema.
            table (str):  table name.
            report_columns (list): list of columns for report.
            formated_date (str): execution formated date.

        Returns:
            str: returns query
        
        """
        return f"""
        select 
        {",".join(report_columns)}
        from {schema}.{table} 
        where app_execution_id = '{formated_date}'
        """

    def get_delete_detail(self,date:str,execution_id:str)->str:
        """Returns query to delete detail
        
        Args:
            date (str):  month of execution.
            execution_id (str):  execution id.

        Returns:
            str: returns query
        
        """
        return f"delete from operational.mh_transaction_scheme_fee where app_customer_code = '{self.client}' and scheme_fee_execution_month = '{date}' and app_execution_id != '{execution_id}'"

    def get_delete_report(self,month_name:str,execution_id)->str:
        """Returns query to delete report
        
        Args:
            month_name (str):  month of execution.
            execution_id (str):  execution id.

        Returns:
            str: returns query
        """
        return f"delete from operational.mh_monthly_scheme_fee where customer_code = '{self.client}' and report_month = '{month_name}' and app_execution_id != '{execution_id}'"
    
    def get_delete_report_legacy(self,month_name:str,execution_id)->str:
        """Returns query to delete legacy report
        
        Args:
            month_name (str):  month of execution.
            execution_id (str):  execution id.

        Returns:
            str: returns query
        """
        return f"delete from operational.mh_monthly_scheme_fee_legacy where customer_code = '{self.client}' and report_month = '{month_name}' and app_execution_id != '{execution_id}'"

    def get_delete_sumary(self,month_name:str,execution_id)-> str:
        """Returns query to delete sumary
        
        Args:
            month_name (str):  month of execution.
            execution_id (str):  execution id.

        Returns:
            str: returns query
        
        """
        return f"delete from operational.mh_scheme_fee_sumary where report_client_code = '{self.client}' and report_month = '{month_name}' and app_execution_id != '{execution_id}'"

    def get_insert_into_sumary(self,formated_date:str,month_name:str,inserted_into_detail:str,inserted_into_header:str,s3_route:str)->str:
        """Returns query to insert into sumary
        
        Args:
            formated_date (str): formated execution date.
            month_name (str):  report month name.
            inserted_into_detail (str): rows inserted.
            inserted_into_header (str): rows inserted.
            s3_route (str): route to output report.

        Returns:
            str: returns query
        
        """
        return f"""
                insert into operational.mh_scheme_fee_sumary 
                (app_execution_id,report_month,report_client_code,number_of_inserted_rows,number_of_groups,number_of_updated_rows,s3_route)
                values('{formated_date}','{month_name}','{self.client}',{inserted_into_detail},{inserted_into_header},0,'{s3_route}')
                """
    
    def get_update_from_temp(self,temp_schem:str,temp_table:str,month_name:str, app_execution_id:str)->str:
        """Returns query to update from temp
        
        Args:
            temp_schem (str): temporal schema.
            temp_table (str): temporal table name.
            month_name (str): nmonth of report.
            app_execution_id(str): id of report execution.

        Returns:
            str: returns query
        """
        return f"""
            update operational.mh_monthly_scheme_fee sf  set 
                transaction_scheme_fee_cost         =   x.txn_sfc,
                estimated_scheme_fee_amount         =   x.est_sch_fee_amt,
                unitary_scheme_fee_cost             =   x.unt_sfc,
                unitary_estimated_scheme_fee_amount =   x.unt_est_sch_fee_amt
            from (select * from {temp_schem}.{temp_table} ) x 
            where sf.app_execution_id= '{app_execution_id}'
            and sf.customer_code =  '{self.client}'
            and sf.report_month ='{month_name}'
            and sf.app_id = x.app_id
            """

    def get_drop_if_exists(self,temp_schem:str,temp_table:str)->str:
        """Returns query to drop temporal if exists

        Args:
            temp_schem (str): temporal schema.
            temp_table (str): temporal table name.

        Returns:
            str: returns query
        """
        return f"drop table if exists {temp_schem}.{temp_table}"

    def get_update_detail(self,iss_acq_ind:str,partition_date:str,month_name:str, app_execution_id:str)->str:
        """Returns query to update from temp
        
        Args:
            iss_acq_ind (str): business mode values (IN, OUT).
            partition_date (str): partition date table (YYYYMMDD).
            month_name (str): month of report (YYYYMM) 
            app_execution_id(str): id of report execution from summary.

        Returns:
            str: returns query
        """
        return f"""                             
            update operational.mh_transaction_scheme_fee_{self.client}_{iss_acq_ind}_{partition_date} t1
            set 
                transaction_scheme_fee_cost         = t2.transaction_scheme_fee_cost,
                unitary_scheme_fee_cost             = t2.unitary_scheme_fee_cost,
                estimated_scheme_fee_cost           = t2.estimated_scheme_fee_amount,
                unitary_estimated_scheme_fee_cost   = t2.unitary_estimated_scheme_fee_amount
            from operational.mh_monthly_scheme_fee_{self.client}_report_{month_name} t2
            where   coalesce(t2.app_execution_id, '') = coalesce(t1.app_execution_id, '') and
                    coalesce(t2.report_month, '') = coalesce(t1.scheme_fee_execution_month, '') and
                    coalesce(t2.report_currency, '') = coalesce(t1.report_currency, '') and
                    coalesce(t1.app_customer_code, '') = coalesce(t1.app_customer_code, '') and
                    coalesce(t2.bank_country_code, '') = coalesce(t1.bank_country, '') and
                    coalesce(t2.business_mode, '') = coalesce(t1.business_mode_id, '') and
                    coalesce(t2.brand, '') = coalesce(t1.transaction_brand, '') and
                    coalesce(t2.size_ticket, '') = coalesce(t1.size_ticket, '') and
                    coalesce(t2.product_id, '') = coalesce(t1.product_id, '') and
                    coalesce(t2.range_program_id, '') = coalesce(t1.range_program_id, '') and
                    coalesce(t2.founding_source, '') = coalesce(t1.account_funding_source, '') and
                    coalesce(t2.transaction_type, '') = coalesce(t1.business_transaction_type_id, '') and
                    coalesce(t2.jurisdiction, '') = coalesce(t1.jurisdiction, '') and
                    coalesce(t2.reversal_indicator, '') = coalesce(t1.reversal_indicator, '') and
                    coalesce(t2.currency_local_indicator, '') = coalesce(t1.currency_local_indicator, '') and
                    coalesce(t2.card_present_indicator, '') = coalesce(t1.motoec_indicator, '') and
                    coalesce(t2.account_number, '') = coalesce(t1.account_number, '') and
                    coalesce(t2.merchant_country_code, '') = coalesce(t1.merchant_country_code, '') and
                    t1.app_execution_id='{app_execution_id}'
            """
    
    
    def get_update_sumary(self,rows_updated:str,month_name:str)->str:
        """returns query for update sumary table.

        Args:
            rows_updated (str): rows updated.
            month_name (str): month of report.

        Returns:
            str: returns query
        
        """
        return f"update operational.mh_scheme_fee_sumary set number_of_updated_rows = {rows_updated} where report_month = '{month_name}' and  report_client_code = '{self.client}'"