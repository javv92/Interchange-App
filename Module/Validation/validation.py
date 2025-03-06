from datetime import datetime, timedelta
from Module.Persistence.connection import connect_to_postgreSQL as bdpostgre
import Module.Logs.logs as log
import numpy as np


class Validation:
    """Class for validation of files and process
    
    Params:
        brand (str): brand.
        client (str): client code.
        log_name (str): name of log file.
    """
    def __init__(self, brand:str, client:str, log_name: str) -> None:
        self.brand = brand
        self.client = client
        self.ps = bdpostgre()
        self.log_name = log_name
        self.module = "VALIDATION"

    def process_validation_interpretation(self):
        """Adds the rows in the dh_visa_validation_interpretation and dh_mastercard_validation_interpretation tables resulting from the validation of the visa and mastercard interpretation that serves as a report"""

        db = bdpostgre()
        schem = "temporal"
        module = "OPERATIONAL"
        module_name = "VALIDATION"
        date_now = datetime.now().strftime("%Y-%m-%d")

        log.logs().exist_file(
            module,
            self.client,
            "VISA",
            self.log_name,
            "EXECUTING THE INTERPRETATION VALIDATION PROCESS",
            "INFO",
            "in process",
            module_name,
        )

        table_new_visa = f"tmp_visa_validation_interpretation_{self.client.lower()}"

        db.drop_table(f"temporal.{table_new_visa}")
        validation_interpretation_visa = f"""
        create table temporal.{table_new_visa} as 
        WITH transactions AS ( 
        (select
            app_processing_date, app_type_file , count(1) as "records_row_loaded"
        from
            operational.dh_visa_transaction
        group by
            app_processing_date, app_type_file)
        UNION ALL
        (select
            app_processing_date, app_type_file , count(1) as "records_row_loaded"
        from
            operational.dh_visa_transaction_vss_110
        group by
            app_processing_date, app_type_file)
        UNION ALL 
        (select
            app_processing_date, app_type_file , count(1) as "records_row_loaded"
        from
            operational.dh_visa_transaction_vss_120
        group by
            app_processing_date, app_type_file)
        UNION ALL 
        (select
            app_processing_date, app_type_file, count(1) as "records_row_loaded"
        from
            operational.dh_visa_transaction_vss_130
        group by
            app_processing_date, app_type_file)
        UNION ALL 
        (select
            app_processing_date, app_type_file, count(1) as "records_row_loaded"
        from
            operational.dh_visa_transaction_vss_140
        group by
            app_processing_date, app_type_file)
        UNION ALL 
        (select
            app_processing_date, app_type_file, count(1) as "records_row_loaded"
        from
            operational.dh_visa_transaction_sms
        group by
            app_processing_date, app_type_file)
        UNION ALL 
        (select
            app_processing_date, app_type_file, count(1) as "records_row_loaded"
        from
            operational.dh_visa_transaction_trailer
        group by
            app_processing_date, app_type_file)
        UNION ALL 
        (select
            app_processing_date, app_type_file,	count(1) as "records_row_loaded"
        from
            operational.dh_visa_transaction_header
        group by
            app_processing_date, app_type_file) 
        ), control_file AS
        (
        select
            cf_a.file_date, cf_a.customer,
            cf_a.file_type,
            cf_b.last_processed_date ,
            cf_a.unique_files,
            cf_a.file_times_processed,
            cf_b.total_records_files
        from
            (
            select
                file_date ,
                customer,
                file_type ,
                count(distinct code) as "unique_files" ,
                (count(file_date)/ count(distinct code)) as "file_times_processed"
            from
                control.t_control_file
            where
                (customer = '{self.client}'
                and brand = 'VI'
                and (description_status = 'PROCESSED'
                or description_status = 'REVISION' ))
            group by
                customer,
                file_date,
                file_type) cf_a
        inner join (
            select
                tmp.file_date,
                max(tmp.process_date) as "last_processed_date" ,
                sum(tmp.records_number) as "total_records_files"
            from
                (
                select
                    file_date,
                    code,
                    process_file_name ,
                    brand,
                    file_type,
                    customer ,
                    process_date,
                    execution_id,
                    records_number,
                    row_number() over (partition by "code" order by "process_date" desc, "execution_id" desc) orden
                from
                    control.t_control_file
                where
                    customer = '{self.client}'
                    and description_status = 'PROCESSED') tmp
            where
                tmp.orden = 1
            group by
                tmp.file_date) cf_b 
        on cf_a.file_date = cf_b.file_date 
        )
        select
            (select coalesce(max(app_id),0) from operational.dh_visa_validation_interpretation) + 1 app_id,
            cf.file_date as app_processing_date,
            cf.customer as app_customer_code,
            cf.file_type as app_type_file,
            cf.last_processed_date ,
            cf.unique_files,
            cf.file_times_processed,
            cf.total_records_files,
            tr.total_row_loaded
        from
            control_file cf
        inner join (
            select
                app_processing_date,
                app_type_file,
                sum(records_row_loaded) as total_row_loaded
            from
                Transactions r
            group by
                app_processing_date,
                app_type_file ) tr 
        on cf.file_date = tr.app_processing_date
        and cf.file_type = tr.app_type_file 
        WHERE cf.last_processed_date = '{date_now}'
        """
 
        report_table_visa = db.execute_block(validation_interpretation_visa)
        report_info_visa = db.select_to_df_object(
            f"select * from temporal.{table_new_visa}"
        )

        report_info_visa.index = np.arange(1, len(report_info_visa) + 1)
        report_info_visa["app_id"] = report_info_visa.index

        if report_info_visa.empty:
            log.logs().exist_file(
                "OPERATIONAL",
                self.client,
                "VISA",
                self.log_name,
                "no interpretation run information detected",
                "INFO",
                "finished",
                module_name,
            )

            log.logs().exist_file(
                module,
                self.client,
                "MASTERCARD",
                self.log_name,
                "INFO",
                "validation finished",
                "inserted rows: " + str(len(report_info_visa.index)),
                module_name,
            )
        else:

            list_of_columns = list(report_info_visa)
            list_of_columns = ",".join(list_of_columns)
            sql2 = f"""
            insert into operational.dh_visa_validation_interpretation({list_of_columns}) select {list_of_columns} from
            {schem}.{table_new_visa} """
            rs = 0
            rs = db.execute_block(sql2)
            rs = 0
            db.drop_table(f"{schem}.{table_new_visa}")

            log.logs().exist_file(
                module,
                self.client,
                "VISA",
                self.log_name,
                "INFO",
                "validation finished",
                "inserted rows: " + str(len(report_info_visa.index)),
                module_name,
            )

        log.logs().exist_file(
            module,
            self.client,
            "MASTERCARD",
            self.log_name,
            "EXECUTING THE INTERPRETATION VALIDATION PROCESS",
            "INFO",
            "in process",
            module_name,
        )

        table_new_mastercard = (
            f"tmp_mastercard_validation_interpretation_{self.client.lower()}"
        )
        db.drop_table(f"temporal.{table_new_mastercard}")
        validation_interpretation_mastercard = f"""
        create table temporal.{table_new_mastercard} as 
        WITH transactions AS (
        (select
            app_processing_date, app_type_file, count(1) as "records_row_loaded"
        from
            operational.dh_mastercard_data_element
        group by
            app_processing_date, app_type_file)
        ), control_file AS
        (
        select
            cf_a.file_date, cf_a.customer,
            cf_a.file_type,
            cf_b.last_processed_date ,
            cf_a.unique_files,
            cf_a.file_times_processed,
            cf_b.total_records_files
        from
            (
            select
                file_date ,
                customer,
                file_type ,
                count(distinct code) as "unique_files" ,
                (count(file_date)/ count(distinct code)) as "file_times_processed"
            from
                control.t_control_file
            where 
                (customer = '{self.client}'
                and brand = 'MC'
                and (description_status = 'PROCESSED'
                or description_status = 'REVISION' ))
            group by
                customer,
                file_date,
                file_type) cf_a
        inner join (
            select
                tmp.file_date,
                max(tmp.process_date) as "last_processed_date" ,
                sum(tmp.records_number) as "total_records_files"
            from
                (
                select
                    file_date,
                    code,
                    process_file_name ,
                    brand,
                    file_type,
                    customer ,
                    process_date,
                    execution_id,
                    records_number,
                    row_number() over (partition by "code" order by "process_date" desc, "execution_id" desc) orden
                from
                    control.t_control_file
                where
                    customer = '{self.client}'
                    and description_status = 'PROCESSED') tmp
            where
                tmp.orden = 1
            group by
                tmp.file_date) cf_b
        on cf_a.file_date = cf_b.file_date
        )
            select
            (select coalesce(max(app_id),0) from operational.dh_mastercard_validation_interpretation) + 1 app_id,
            cf.file_date as app_processing_date,
            cf.customer as app_customer_code,
            cf.file_type as app_type_file,
            cf.last_processed_date ,
            cf.unique_files,
            cf.file_times_processed,
            cf.total_records_files,
            tr.total_row_loaded
        from
            control_file cf
        inner join (
            select
                app_processing_date,
                app_type_file,
                sum(records_row_loaded) as total_row_loaded
            from
                Transactions r
            group by
                app_processing_date,
                app_type_file ) tr 
        on cf.file_date = tr.app_processing_date
        and cf.file_type = tr.app_type_file 
        WHERE cf.last_processed_date = '{date_now}'
        """
        report_table_mc = db.execute_block(validation_interpretation_mastercard)

        report_info_mc = db.select_to_df_object(
            f"select * from temporal.{table_new_mastercard}"
        )
        report_info_mc.index = np.arange(1, len(report_info_mc) + 1)
        report_info_mc["app_id"] = report_info_mc.index
        if report_info_mc.empty:
            log.logs().exist_file(
                "OPERATIONAL",
                self.client,
                "MASTERCARD",
                self.log_name,
                "NO INTERPRETATION RUN INFORMATION DETECTED",
                "INFO",
                "finished",
                module_name,
            )

            log.logs().exist_file(
                module,
                self.client,
                "MASTERCARD",
                self.log_name,
                "FINISHED VALIDATION",
                "INFO",
                "inserted rows: " + str(len(report_info_mc.index)),
                module_name,
            )

        else:

            list_of_columns = list(report_info_mc)
            list_of_columns = ",".join(list_of_columns)

            sql2 = f"""
            insert into operational.dh_mastercard_validation_interpretation({list_of_columns}) select {list_of_columns} from
            {schem}.{table_new_mastercard} """
            rs = 0
            rs = db.execute_block(sql2)
            db.drop_table(f"{schem}.{table_new_mastercard}")

            log.logs().exist_file(
                module,
                self.client,
                "MASTERCARD",
                self.log_name,
                "INTERPRETATION VALIDATION FINISHED",
                "INFO",
                "inserted rows: " + str(len(report_info_mc.index)),
                module_name,
            )

    def process_validation_visa_interchange(
        self, string_date: str = None, type_file: str = None
    ) -> str:
        """VISA validation interchange
        
        Args:
            string_date (str): string date of file.
            type_file (str): type of file.

        Returns:
            str: Message
        """
        table_scheme = "operational"
        table_name = "dh_visa_validation_interchange"
        customer_code = self.client
        business_mode = "1"
        report_type = "130"
        str_business_mode = "acquiring"
        settlement_string_date = string_date
        if string_date == None:
            string_date = datetime.now().strftime("%Y%m%d")
        if type_file.lower() == "in":
            business_mode = "2"
            str_business_mode = "issuing"
        list_max_id = self.ps.select(
            f"{table_scheme}.{table_name}", cols="max(app_id) app_id"
        )
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "VALIDATION OF VISA INTERCHANGE FILE",
            "INFO",
            "loading visa validation interchange" + table_scheme + "." + table_name,
            self.module,
        )

        query_tmp = f"""
        with interchange_assigned as
        (
            SELECT 
            ti.app_id,ti.app_hash_file,ti.app_processing_date
            ,cf.business_mode
            ,cf.jurisdiction_assigned
            ,ti.intelica_id
            ,vt.transaction_code
            ,vt.purchase_date
            ,trim(ir.fee_descriptor) fee_descriptor
            ,ti.amount_transaction
            ,cur.currency_alphabetic_code currency_transaction
            ,ex.exchange_value
            ,ti.amount_transaction * coalesce(ex.exchange_value::numeric,1) local_amount_transaction
            ,cur_local.currency_alphabetic_code local_currency_transaction
            ,(ti.calculated_value) * coalesce(ex.exchange_value::numeric,1) local_calculated_value
            ,vt.destination_amount
            ,vt.destination_currency_code
            ,cf.settlement_report_amount
            ,cf.settlement_report_currency_code
            ,btt.short_description business_transaction_type
            ,btc.short_description business_transaction_cycle
            ,cf.reversal_indicator reversal_flag
            FROM operational.dh_visa_interchange_{customer_code}_{type_file}_{string_date} ti
            inner JOIN operational.M_INTERCHANGE_RULES_VISA IR 
            ON (IR.INTELICA_ID::numeric = TI.INTELICA_ID and ir.region_country_code = ti.region_country_code)
            inner join operational.dh_visa_transaction_calculated_field_{customer_code} cf
            on (ti.app_id=cf.app_id and ti.app_hash_file=cf.app_hash_file)
            inner join operational.dh_visa_transaction_{customer_code} vt
            on (vt.app_id = ti.app_id and vt.app_hash_file=ti.app_hash_file)
            left join control.t_customer cus 
            on (cus.code = ti.app_customer_code)
            left join operational.m_currency cur 
            on (cur.currency_numeric_code = ti.currency_transaction::integer)
            left join operational.m_currency cur_local 
            on (cur_local.currency_alphabetic_code = cus.local_currency_code)
            left join operational.dh_exchange_rate ex 
            on ex.app_processing_date = vt.purchase_date
            and currency_from_code = ti.currency_transaction
            and currency_to_code = cur_local.currency_numeric_code::text
            and ex.brand = 'VISA'
            left join operational.m_visa_business_transaction_type btt
            on (btt.business_transaction_type_id::int = cf.business_transaction_type)
            left join operational.m_visa_business_transaction_cycle btc
            on (btc.business_transaction_cycle_id::int = cf.business_transaction_cycle)
            WHERE to_date('{settlement_string_date}','yyyymmdd') BETWEEN IR.VALID_FROM AND COALESCE(IR.VALID_UNTIL,CURRENT_DATE)
        ),vss_report as
        (
            SELECT 
            t.app_processing_date,
            report_type,
            t2.aggregation_level,
            summary_level_130,
            case 
                when jurisdiction_code_130 = '00' then '9'
                else
                    case 
                        when t.source_country_code_130<>t.destination_country_code_130
                            then c.visa_region_code::text
                    else 
                        c.country_code
                    end 
            end jurisdiction_assigned,
            case t.business_mode_130 when '2' then 'issuing' when '1' then 'acquiring' end business_mode,
            trim(t.fee_level_descriptor) fee_level_descriptor,
            cu.currency_alphabetic_code  vss_currency,
            btt.short_description business_transaction_type,
            btc.short_description business_transaction_cycle,
            case when t.reversal_indicator_130 = 'N' then 0 else 1 end reversal_flag,
            sum(cast(case when trim(count_130) = '' then '0' else count_130 end as int))  vss_count,
            sum(interchange_amount_settlement_currency_130::numeric) vss_amount,
            sum(reimbursement_fee_debits_settlement_currency::numeric + reimbursement_fee_credits_settlement_currency::numeric) irf_amount
            from operational.dh_visa_transaction_vss_130_{customer_code}_in_{settlement_string_date} T
            inner join operational.dh_visa_transaction_vss_calculated_field T2
            on t.app_id = t2.app_id and t.app_hash_file = t2.app_hash_file and t.app_customer_code = t2.app_customer_code
            left join operational.m_country c
            on (c.country_numeric =case when trim(t."source_country_code_130")<>'' then case 
                            when t."business_mode_130" = '2' then t."destination_country_code_130" 
                        else t."source_country_code_130" end end::numeric)
            left join operational.m_currency cu
            on (cu.currency_numeric_code=case when trim(t.settlement_currency_code_130)<>'' then t.settlement_currency_code_130::numeric end)
            left join operational.m_visa_business_transaction_type btt
            on (btt.business_transaction_type_code = T.business_transaction_type_130)
            left join operational.m_visa_business_transaction_cycle btc
            on (btc.business_transaction_cycle_code = T.business_transaction_cycle_130)
            where
            trim(t.business_mode_130)='{business_mode}' and
            t2.aggregation_level = 0
            and length(trim(fee_level_descriptor))>0
            and not((trim(t.business_transaction_type_130)='310' and t.business_mode_130 = '1') or (trim(t.business_transaction_type_130)::integer>=519 and t.business_mode_130 = '1') or (trim(t.business_transaction_cycle_130) in ('7','8','0') and t.business_mode_130='1'))
            and not ((trim(t.business_transaction_type_130)::integer>=519 and t.business_mode_130 = '2') or (trim(t.business_transaction_cycle_130) in ('7','8','0') and t.business_mode_130='2'))
            and t.return_indicator_130 in ('N',' ')
            group by 1,2,3,4,5,6,7,8,9,10,11
        ), interchange_report as
        (   
            select 
            jurisdiction_assigned,
            business_mode,app_processing_date,fee_descriptor,local_currency_transaction,business_transaction_type,business_transaction_cycle,reversal_flag,count(1) transaction_count,sum(local_amount_transaction) local_amount_transaction, sum(local_calculated_value) local_calculated_value
            from interchange_assigned
            group by 1,2,3,4,5,6,7,8
        ), interchange_rules_visa_order as 
        (
            select jurisdiction,region_country_code,fee_descriptor,min(intelica_id::numeric) order_rule from operational.m_interchange_rules_visa
            where to_date('{settlement_string_date}','yyyymmdd') BETWEEN VALID_FROM AND COALESCE(VALID_UNTIL,CURRENT_DATE)
            group by 1,2,3
        ), join_vss_interchange as
        (  select
            iro.region_country_code jurisdiction_rule
            ,iro.order_rule
            ,iro.fee_descriptor fee_descriptor_rule
            ,coalesce(vss.app_processing_date,itx.app_processing_date) report_processing_date
            ,'{str_business_mode}' report_business_mode
            ,'{report_type}' report_type
            ,vss.jurisdiction_assigned vss_jurisdiction
            ,vss.business_mode vss_business_mode
            ,vss.fee_level_descriptor vss_fee_descriptor
            ,vss.business_transaction_type settlement_business_transaction_type
            ,vss.business_transaction_cycle settlement_business_transaction_cycle
            ,vss.reversal_flag settlement_reversal_flag
            ,vss.vss_count
            ,vss.vss_amount
            ,vss.irf_amount
            ,itx.jurisdiction_assigned itx_jurisdiction
            ,itx.business_mode itx_business_mode
            ,itx.fee_descriptor itx_fee_descriptor
            ,itx.business_transaction_type interchange_business_transaction_type
            ,itx.business_transaction_cycle interchange_business_transaction_cycle
            ,itx.reversal_flag interchange_reversal_flag
            ,itx.transaction_count itx_count
            ,itx.local_amount_transaction itx_amount
            ,itx.local_calculated_value itx_irf_amount
            ,coalesce(vss.vss_count,0) - coalesce(itx.transaction_count,0) diff_count
            ,coalesce(vss.vss_amount,0) - coalesce(itx.local_amount_transaction,0) diff_amount
            ,coalesce(vss.irf_amount,0) - coalesce(itx.local_calculated_value,0) diff_irf_amount
            ,abs(coalesce(itx.transaction_count,0) - coalesce(vss.vss_count,0)) diff_count_abs
            ,case when coalesce(itx.transaction_count,0) >= coalesce(vss.vss_count,0) then  coalesce(vss.vss_count,0) else coalesce(itx.transaction_count,0) end metric_count
            ,case when coalesce(itx.transaction_count,0) < coalesce(vss.vss_count,0) then 0 else coalesce(itx.transaction_count,0) - coalesce(vss.vss_count,0) end metric_excess_count
            from vss_report vss
            full join interchange_report itx
            on (
            vss.jurisdiction_assigned=itx.jurisdiction_assigned and
            vss.business_mode = itx.business_mode and
            vss.fee_level_descriptor = itx.fee_descriptor and 
            coalesce(vss.business_transaction_type,'') = coalesce(itx.business_transaction_type,'') and 
            coalesce(vss.business_transaction_cycle,'') = coalesce(itx.business_transaction_cycle,'') and 
            vss.reversal_flag = itx.reversal_flag
            )
            left join interchange_rules_visa_order iro
            on (iro.region_country_code = coalesce(vss.jurisdiction_assigned,itx.jurisdiction_assigned) and iro.fee_descriptor = coalesce(vss.fee_level_descriptor,itx.fee_descriptor))
        ),error_vss_interchange as
        (
            select vi.*,((diff_count_abs - metric_excess_count) / coalesce(vss_count,1)::numeric) absolute_error from join_vss_interchange vi
        ),agg_error_vss_interchange as
        (
            select
            sum(vss_count) vss_count
            ,sum(diff_count_abs) - sum(metric_excess_count) diff
            ,((sum(diff_count_abs) - sum(metric_excess_count)) / sum(vss_count)) absolute_error
            from join_vss_interchange r
        )
        select
        replace(lower('{list_max_id[0]['app_id']}'),'none','0')::numeric + row_number()over(order by jurisdiction_rule,order_rule) app_id
        ,'{self.client}' app_customer_code
        ,upper('{type_file}') app_type_file
        ,'' app_hash_file
        ,report_processing_date app_processing_date
        ,jurisdiction_rule
        ,order_rule
        ,fee_descriptor_rule descriptor_rule
        ,report_business_mode
        ,report_type
        ,vss_jurisdiction settlement_jurisdiction
        ,vss_business_mode settlement_business_mode
        ,vss_fee_descriptor settlement_fee_descriptor
        ,settlement_business_transaction_type
        ,settlement_business_transaction_cycle
        ,settlement_reversal_flag
        ,vss_count settlement_count
        ,vss_amount settlement_amount
        ,irf_amount settlement_irf_amount
        ,itx_jurisdiction interchange_jurisdiction
        ,itx_business_mode interchange_business_mode
        ,itx_fee_descriptor interchange_fee_descriptor
        ,interchange_business_transaction_type
        ,interchange_business_transaction_cycle
        ,interchange_reversal_flag
        ,itx_count interchange_count
        ,itx_amount interchange_amount
        ,itx_irf_amount interchange_irf_amount
        ,diff_count
        ,diff_amount
        ,diff_irf_amount
        ,diff_count_abs
        ,metric_count
        ,metric_excess_count
        ,absolute_error
        from 
        error_vss_interchange
        """
        table_scheme_tmp = "temporal"
        table_name_tmp = (
            f"{customer_code}_{type_file}_{string_date}_visa_validation_interchange"
        )
        table_tmp = f"{table_scheme_tmp}.{table_name_tmp}"
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "VALIDATION OF VISA INTERCHANGE",
            "INFO",
            message_drop,
            self.module,
        )
        message_create = self.ps.create_table_from_select(query_tmp, table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "VALIDATION OF VISA INTERCHANGE",
            "INFO",
            message_create,
            self.module,
        )

        message_insert = self.ps.insert_from_table(
            table_scheme_tmp, table_name_tmp, table_scheme, table_name
        )
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "VALIDATION OF VISA INTERCHANGE FILE",
            "INFO",
            message_insert,
            self.module,
        )

        return "finished"

    def process_validation_visa_sms_interchange(
        self, string_date: str = None, type_file: str = None
    ) -> str:
        """VISA SMS validation interchange
        
        Args:
            string_date (str): string date of file.
            type_file (str): type of file.

        Returns:
            str: Message
        """
        table_scheme = "operational"
        table_name = "dh_visa_sms_validation_interchange"
        customer_code = self.client
        business_mode = "1"
        report_type = "130"
        str_business_mode = "acquiring"
        settlement_string_date = string_date
        if string_date == None:
            string_date = datetime.now().strftime("%Y%m%d")
        list_max_id = self.ps.select(
            f"{table_scheme}.{table_name}", cols="max(app_id) app_id"
        )
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "VALIDATION OF VISA SMS INTERCHANGE FILE",
            "INFO",
            "loading visa sms validation interchange" + table_scheme + "." + table_name,
            self.module,
        )

        query_tmp = f"""
        with interchange_assigned as
        (
            SELECT 
            ti.app_id,ti.app_hash_file,ti.app_processing_date
            ,cf.business_mode
            ,cf.jurisdiction_assigned
            ,ti.intelica_id
            ,vt.transaction_code
            ,vt.purchase_date
            ,trim(ir.fee_descriptor) fee_descriptor
            ,ti.amount_transaction
            ,cur.currency_alphabetic_code currency_transaction
            ,ex.exchange_value
            ,ti.amount_transaction * coalesce(ex.exchange_value::numeric,1) local_amount_transaction
            ,cur_local.currency_alphabetic_code local_currency_transaction
            ,(ti.calculated_value) * coalesce(ex.exchange_value::numeric,1) local_calculated_value
            ,vt.destination_amount
            ,vt.destination_currency_code
            ,cf.settlement_report_amount
            ,cf.settlement_report_currency_code
            ,btt.short_description business_transaction_type
            ,btc.short_description business_transaction_cycle
            ,cf.reversal_indicator reversal_flag
            FROM operational.dh_visa_sms_interchange_{customer_code}_{type_file}_{string_date} ti
            inner JOIN operational.M_INTERCHANGE_RULES_VISA IR 
            ON (IR.INTELICA_ID::numeric = TI.INTELICA_ID and ir.region_country_code = ti.region_country_code)
            inner join operational.dh_visa_transaction_calculated_field_{customer_code} cf
            on (ti.app_id=cf.app_id and ti.app_hash_file=cf.app_hash_file)
            inner join operational.dh_visa_transaction_{customer_code} vt
            on (vt.app_id = ti.app_id and vt.app_hash_file=ti.app_hash_file)
            left join control.t_customer cus 
            on (cus.code = ti.app_customer_code)
            left join operational.m_currency cur 
            on (cur.currency_numeric_code = ti.currency_transaction::integer)
            left join operational.m_currency cur_local 
            on (cur_local.currency_alphabetic_code = cus.local_currency_code)
            left join operational.dh_exchange_rate ex 
            on ex.app_processing_date = vt.purchase_date
            and currency_from_code = ti.currency_transaction
            and currency_to_code = cur_local.currency_numeric_code::text
            and ex.brand = 'VISA'
            left join operational.m_visa_business_transaction_type btt
            on (btt.business_transaction_type_id::int = cf.business_transaction_type)
            left join operational.m_visa_business_transaction_cycle btc
            on (btc.business_transaction_cycle_id::int = cf.business_transaction_cycle)
            WHERE to_date('{string_date}','yyyymmdd') BETWEEN IR.VALID_FROM AND COALESCE(IR.VALID_UNTIL,CURRENT_DATE)
        ),vss_report as
        (
            SELECT 
            t.app_processing_date,
            report_type,
            t2.aggregation_level,
            summary_level_130,
            case 
                when jurisdiction_code_130 = '00' then '9'
                else
                    case 
                        when t.source_country_code_130<>t.destination_country_code_130
                            then c.visa_region_code::text
                    else 
                        c.country_code
                    end 
            end jurisdiction_assigned,
            case t.business_mode_130 when '2' then 'issuing' when '1' then 'acquiring' end business_mode,
            trim(t.fee_level_descriptor) fee_level_descriptor,
            cu.currency_alphabetic_code  vss_currency,
            btt.short_description business_transaction_type,
            btc.short_description business_transaction_cycle,
            case when t.reversal_indicator_130 = 'N' then 0 else 1 end reversal_flag,
            sum(cast(case when trim(count_130) = '' then '0' else count_130 end as int))  vss_count,
            sum(interchange_amount_settlement_currency_130::numeric) vss_amount,
            sum(reimbursement_fee_debits_settlement_currency::numeric + reimbursement_fee_credits_settlement_currency::numeric) irf_amount
            from operational.dh_visa_transaction_vss_130_{customer_code}_{type_file}_{settlement_string_date} T
            inner join operational.dh_visa_transaction_vss_calculated_field T2
            on t.app_id = t2.app_id and t.app_hash_file = t2.app_hash_file and t.app_customer_code = t2.app_customer_code
            left join operational.m_country c
            on (c.country_numeric =case when trim(t."source_country_code_130")<>'' then case 
                            when t."business_mode_130" = '2' then t."destination_country_code_130" 
                        else t."source_country_code_130" end end::numeric)
            left join operational.m_currency cu
            on (cu.currency_numeric_code=case when trim(t.settlement_currency_code_130)<>'' then t.settlement_currency_code_130::numeric end)
            left join operational.m_visa_business_transaction_type btt
            on (btt.business_transaction_type_code = T.business_transaction_type_130)
            left join operational.m_visa_business_transaction_cycle btc
            on (btc.business_transaction_cycle_code = T.business_transaction_cycle_130)
            where
            trim(t.business_mode_130)='{business_mode}' and
            t2.aggregation_level = 0
            and t2.aggregation_level = 0
            and length(trim("fee_level_descriptor"))>0
            and trim(t.business_transaction_type_130)='310'
            and trim(t.business_transaction_cycle_130) not in ('7','8','0')
            and t.return_indicator_130 in ('N',' ')
            group by 1,2,3,4,5,6,7,8,9,10,11
        ), interchange_report as
        (   
            select 
            jurisdiction_assigned,
            business_mode,app_processing_date,fee_descriptor,local_currency_transaction,business_transaction_type,business_transaction_cycle,reversal_flag,count(1) transaction_count,sum(local_amount_transaction) local_amount_transaction, sum(local_calculated_value) local_calculated_value
            from interchange_assigned
            group by 1,2,3,4,5,6,7,8
        ), interchange_rules_visa_order as 
        (
            select jurisdiction,region_country_code,fee_descriptor,min(intelica_id::numeric) order_rule from operational.m_interchange_rules_visa
            where to_date('{string_date}','yyyymmdd') BETWEEN VALID_FROM AND COALESCE(VALID_UNTIL,CURRENT_DATE)
            group by 1,2,3
        ), join_vss_interchange as
        (  select
            iro.region_country_code jurisdiction_rule
            ,iro.order_rule
            ,iro.fee_descriptor fee_descriptor_rule
            ,coalesce(vss.app_processing_date,itx.app_processing_date) report_processing_date
            ,'{str_business_mode}' report_business_mode
            ,'{report_type}' report_type
            ,vss.jurisdiction_assigned vss_jurisdiction
            ,vss.business_mode vss_business_mode
            ,vss.fee_level_descriptor vss_fee_descriptor
            ,vss.business_transaction_type settlement_business_transaction_type
            ,vss.business_transaction_cycle settlement_business_transaction_cycle
            ,vss.reversal_flag settlement_reversal_flag
            ,vss.vss_count
            ,vss.vss_amount
            ,vss.irf_amount
            ,itx.jurisdiction_assigned itx_jurisdiction
            ,itx.business_mode itx_business_mode
            ,itx.fee_descriptor itx_fee_descriptor
            ,itx.business_transaction_type interchange_business_transaction_type
            ,itx.business_transaction_cycle interchange_business_transaction_cycle
            ,itx.reversal_flag interchange_reversal_flag
            ,itx.transaction_count itx_count
            ,itx.local_amount_transaction itx_amount
            ,itx.local_calculated_value itx_irf_amount
            ,coalesce(vss.vss_count,0) - coalesce(itx.transaction_count,0) diff_count
            ,coalesce(vss.vss_amount,0) - coalesce(itx.local_amount_transaction,0) diff_amount
            ,coalesce(vss.irf_amount,0) - coalesce(itx.local_calculated_value,0) diff_irf_amount
            ,abs(coalesce(itx.transaction_count,0) - coalesce(vss.vss_count,0)) diff_count_abs
            ,case when coalesce(itx.transaction_count,0) >= coalesce(vss.vss_count,0) then  coalesce(vss.vss_count,0) else coalesce(itx.transaction_count,0) end metric_count
            ,case when coalesce(itx.transaction_count,0) < coalesce(vss.vss_count,0) then 0 else coalesce(itx.transaction_count,0) - coalesce(vss.vss_count,0) end metric_excess_count
            from vss_report vss
            full join interchange_report itx
            on (
            vss.jurisdiction_assigned=itx.jurisdiction_assigned and
            vss.business_mode = itx.business_mode and
            vss.fee_level_descriptor = itx.fee_descriptor and 
            coalesce(vss.business_transaction_type,'') = coalesce(itx.business_transaction_type,'') and 
            coalesce(vss.business_transaction_cycle,'') = coalesce(itx.business_transaction_cycle,'') and 
            vss.reversal_flag = itx.reversal_flag
            )
            left join interchange_rules_visa_order iro
            on (iro.region_country_code = coalesce(vss.jurisdiction_assigned,itx.jurisdiction_assigned) and iro.fee_descriptor = coalesce(vss.fee_level_descriptor,itx.fee_descriptor))
        ),error_vss_interchange as
        (
            select vi.*,((diff_count_abs - metric_excess_count) / coalesce(vss_count,1)::numeric) absolute_error from join_vss_interchange vi
        ),agg_error_vss_interchange as
        (
            select
            sum(vss_count) vss_count
            ,sum(diff_count_abs) - sum(metric_excess_count) diff
            ,((sum(diff_count_abs) - sum(metric_excess_count)) / sum(vss_count)) absolute_error
            from join_vss_interchange r
        )
        select
        replace(lower('{list_max_id[0]['app_id']}'),'none','0')::numeric + row_number()over(order by jurisdiction_rule,order_rule) app_id
        ,'{self.client}' app_customer_code
        ,upper('{type_file}') app_type_file
        ,'' app_hash_file
        ,report_processing_date app_processing_date
        ,jurisdiction_rule
        ,order_rule
        ,fee_descriptor_rule descriptor_rule
        ,report_business_mode
        ,report_type
        ,vss_jurisdiction settlement_jurisdiction
        ,vss_business_mode settlement_business_mode
        ,vss_fee_descriptor settlement_fee_descriptor
        ,settlement_business_transaction_type
        ,settlement_business_transaction_cycle
        ,settlement_reversal_flag
        ,vss_count settlement_count
        ,vss_amount settlement_amount
        ,irf_amount settlement_irf_amount
        ,itx_jurisdiction interchange_jurisdiction
        ,itx_business_mode interchange_business_mode
        ,itx_fee_descriptor interchange_fee_descriptor
        ,interchange_business_transaction_type
        ,interchange_business_transaction_cycle
        ,interchange_reversal_flag
        ,itx_count interchange_count
        ,itx_amount interchange_amount
        ,itx_irf_amount interchange_irf_amount
        ,diff_count
        ,diff_amount
        ,diff_irf_amount
        ,diff_count_abs
        ,metric_count
        ,metric_excess_count
        ,absolute_error
        from 
        error_vss_interchange
        """
        table_scheme_tmp = "temporal"
        table_name_tmp = (
            f"{customer_code}_{type_file}_{string_date}_visa_sms_validation_interchange"
        )
        table_tmp = f"{table_scheme_tmp}.{table_name_tmp}"
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "VALIDATION OF VISA SMS INTERCHANGE",
            "INFO",
            message_drop,
            self.module,
        )
        message_create = self.ps.create_table_from_select(query_tmp, table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "VALIDATION OF VISA SMS INTERCHANGE",
            "INFO",
            message_create,
            self.module,
        )

        message_insert = self.ps.insert_from_table(
            table_scheme_tmp, table_name_tmp, table_scheme, table_name
        )
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "VISA",
            self.log_name,
            "VALIDATION OF VISA SMS INTERCHANGE FILE",
            "INFO",
            message_insert,
            self.module,
        )

        return "finished"

    def process_validation_mastercard_interchange(
        self, string_date: str = None, type_file: str = None
    ) -> str:
        """MASTERCARD validation interchange
        
        Args:
            string_date (str): string date of file.
            type_file (str): type of file.

        Returns:
            str: Message
        """
        table_scheme = "operational"
        table_name = "dh_mastercard_validation_interchange"
        customer_code = self.client
        business_mode = "A"
        str_business_mode = "acquiring"
        if string_date == None:
            string_date = datetime.now().strftime("%Y%m%d")
        if type_file.lower() == "in":
            business_mode = "I"
            str_business_mode = "issuing"
        list_max_id = self.ps.select(
            f"{table_scheme}.{table_name}", cols="max(app_id) app_id"
        )
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE FILE",
            "INFO",
            "loading mastercard validation interchange"
            + table_scheme
            + "."
            + table_name,
            self.module,
        )
        query_tmp = f"""
        select
        ti.app_id,
        ti.app_hash_file,
        ti.app_processing_date
        ,cf.business_mode 
        ,cf.jurisdiction
        ,cf.jurisdiction_assigned
        ,mt.date_and_time_local_transaction purchase_date
        ,IR.ird
        ,ti.currency_transaction 
        ,ti.amount_transaction * coalesce(ex.exchange_value::numeric,1) local_amount_transaction
        ,cur_local.currency_alphabetic_code local_currency_transaction
        ,ti.calculated_value * coalesce(ex.exchange_value::numeric,1) local_calculated_value
        ,cf.settlement_report_amount
        ,cf.settlement_report_currency_code
        ,btt.short_description business_transaction_type
        ,case when upper(left(mt.message_reversal_indicator,1)) = 'R' then 1 else 0 end reversal_flag
        from operational.dh_mastercard_interchange_{customer_code}_{type_file}_{string_date} ti
        inner join operational.dh_mastercard_calculated_field_{customer_code} cf on (ti.app_id=cf.app_id and ti.app_hash_file=cf.app_hash_file)
        inner join operational.dh_mastercard_data_element_{customer_code} mt on (ti.app_id=mt.app_id and ti.app_hash_file=mt.app_hash_file)
        inner join operational.M_INTERCHANGE_RULES_MC IR on (IR.INTELICA_ID::numeric = TI.INTELICA_ID and ir.region_country_code = ti.region_country_code and ir.ird = ti.ird)
        left join control.t_customer cus on (cus.code = ti.app_customer_code)
        left join operational.m_currency cur on (cur.currency_numeric_code = ti.currency_transaction::integer)
        left join operational.m_currency cur_local on (cur_local.currency_alphabetic_code = cus.local_currency_code)
        left join operational.dh_exchange_rate ex on ex.app_processing_date = mt.date_and_time_local_transaction::date and currency_from_code = ti.currency_transaction and currency_to_code = cur_local.currency_numeric_code::text and ex.brand = 'MASTERCARD'
        left join operational.m_mastercard_business_transaction_type btt on (btt.business_transaction_type_id = left(mt.processing_code,2))
        WHERE to_date('{string_date}','yyyymmdd') BETWEEN IR.VALID_FROM AND COALESCE(IR.VALID_UNTIL,CURRENT_DATE)
        """
        table_scheme_tmp = "temporal"
        table_name_tmp = f"{customer_code}_{type_file}_{string_date}_mastercard_validation_assigned_transactions"
        table_tmp = f"{table_scheme_tmp}.{table_name_tmp}"
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_drop,
            self.module,
        )
        message_create = self.ps.create_table_from_select(query_tmp, table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_create,
            self.module,
        )

        query_tmp = f"""
        select 
        app_processing_date
        ,case upper(trim(reconciled_member_activity)) 
            when 'I' then 'issuing'
            when 'A' then 'acquiring'
        end business_mode
        ,case 
            when reconciled_business_activity_2 = '4' then 'off-us'
            when reconciled_business_activity_2 in ('1','8') then 'interregional'
            when reconciled_business_activity_2 in ('2','3') then 'intraregional'
        end jurisdiction
        ,reconciled_business_activity_4 ird
        ,btt.short_description settlement_business_transaction_type
        ,case when upper(trim(original_reversal_totals_indicator))='O' then 0 else 1 end settlement_reversal_flag
        ,sum(total_transaction_number) settlement_count
        ,sum(amount_net_transaction_in_reconciliation_currency_2) settlement_amount
        ,sum(amount_net_fee_in_reconciliation_currency_2) settlement_irf_amount
        from operational.dh_mastercard_data_element_{customer_code}_in_{string_date} de
        left join operational.m_mastercard_business_transaction_type btt
        on (btt.business_transaction_type_id = de.reconciled_processing_code)
        where 
        app_message_type = '1644' 
        and function_code='685'  
        and reconciled_member_activity='{business_mode}' 
        and (case 
                when reconciled_member_activity = 'I' and reconciled_transaction_function_1 = '1240' and reconciled_file is null then '001' 
                else left(reconciled_file,3) 
            end) in ('001','002') 
        and trim(reconciled_business_activity_4) <> ''
        and reconciled_transaction_function_1 = '1240'
        group by 
        1,2,3,4,5,6
        """
        table_scheme_tmp = "temporal"
        table_name_tmp = f"{customer_code}_{type_file}_{string_date}_mastercard_validation_settlements"
        table_tmp = f"{table_scheme_tmp}.{table_name_tmp}"
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_drop,
            self.module,
        )
        message_create = self.ps.create_table_from_select(query_tmp, table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_create,
            self.module,
        )

        query_tmp = f"""
        select 
        jurisdiction_assigned,
        jurisdiction,
        business_mode,
        app_processing_date,
        ird,
        local_currency_transaction,
        business_transaction_type
        ,reversal_flag
        ,count(ird) transaction_count,
        sum(local_amount_transaction) local_amount_transaction,
        sum(local_calculated_value) local_calculated_value
        from temporal.{customer_code}_{type_file}_{string_date}_mastercard_validation_assigned_transactions
        group by 1,2,3,4,5,6,7,8
        """
        table_scheme_tmp = "temporal"
        table_name_tmp = f"{customer_code}_{type_file}_{string_date}_mastercard_validation_interchange_report"
        table_tmp = f"{table_scheme_tmp}.{table_name_tmp}"
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_drop,
            self.module,
        )
        message_create = self.ps.create_table_from_select(query_tmp, table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_create,
            self.module,
        )

        query_tmp = f"""
        select
        coalesce(setl.app_processing_date,ir.app_processing_date) report_processing_date
        ,'{str_business_mode}' report_business_mode
        ,setl.jurisdiction settlement_jurisdiction
        ,setl.business_mode settlement_business_mode
        ,setl.ird settlement_fee_descriptor
        ,setl.settlement_business_transaction_type
        ,setl.settlement_reversal_flag
        ,setl.settlement_count
        ,setl.settlement_amount
        ,setl.settlement_irf_amount
        ,ir.jurisdiction itx_jurisdiction
        ,ir.business_mode itx_business_mode
        ,ir.ird itx_fee_descriptor
        ,ir.business_transaction_type interchange_business_transaction_type
        ,ir.reversal_flag interchange_reversal_flag
        ,ir.transaction_count itx_count
        ,ir.local_amount_transaction itx_amount
        ,ir.local_calculated_value itx_irf_amount
        ,coalesce(setl.settlement_count,0) - coalesce(ir.transaction_count,0) diff_count
        ,coalesce(setl.settlement_amount,0) - coalesce(ir.local_amount_transaction,0) diff_amount
        ,coalesce(setl.settlement_irf_amount,0) - coalesce(ir.local_calculated_value,0) diff_irf_amount
        ,abs(coalesce(ir.transaction_count,0) - coalesce(setl.settlement_count,0)) diff_count_abs
        ,case when coalesce(ir.transaction_count,0) >= coalesce(setl.settlement_count,0) then  coalesce(setl.settlement_count,0) else coalesce(ir.transaction_count,0) end metric_count
        ,case when coalesce(ir.transaction_count,0) < coalesce(setl.settlement_count,0) then 0 else coalesce(ir.transaction_count,0) - coalesce(setl.settlement_count,0) end metric_excess_count
        from temporal.{customer_code}_{type_file}_{string_date}_mastercard_validation_settlements setl
        full join temporal.{customer_code}_{type_file}_{string_date}_mastercard_validation_interchange_report ir on 
        lower(setl.jurisdiction) = lower(ir.jurisdiction) and
        setl.business_mode = ir.business_mode and
        setl.ird = ir.ird and
        coalesce(setl.settlement_business_transaction_type,'') = coalesce(ir.business_transaction_type,'') and
        setl.settlement_reversal_flag = ir.reversal_flag
        """
        table_scheme_tmp = "temporal"
        table_name_tmp = f"{customer_code}_{type_file}_{string_date}_mastercard_validation_join_interchange"
        table_tmp = f"{table_scheme_tmp}.{table_name_tmp}"
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_drop,
            self.module,
        )
        message_create = self.ps.create_table_from_select(query_tmp, table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_create,
            self.module,
        )

        query_tmp = f"""
        select vi.*,((diff_count_abs - metric_excess_count) / coalesce(settlement_count,1)::numeric) absolute_error from temporal.{customer_code}_{type_file}_{string_date}_mastercard_validation_join_interchange vi
        """
        table_scheme_tmp = "temporal"
        table_name_tmp = f"{customer_code}_{type_file}_{string_date}_mastercard_validation_error_sett_interchange"
        table_tmp = f"{table_scheme_tmp}.{table_name_tmp}"
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_drop,
            self.module,
        )
        message_create = self.ps.create_table_from_select(query_tmp, table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_create,
            self.module,
        )

        query_tmp = f"""
        select
        replace(lower('{list_max_id[0]['app_id']}'),'none','0')::numeric + row_number()over(order by settlement_jurisdiction,settlement_fee_descriptor) app_id
        ,'{self.client}' app_customer_code
        ,upper('{type_file}') app_type_file
        ,'' app_hash_file
        ,report_processing_date app_processing_date
        ,report_business_mode
        ,settlement_jurisdiction
        ,settlement_business_mode
        ,settlement_fee_descriptor
        ,settlement_business_transaction_type
        ,settlement_reversal_flag
        ,settlement_count
        ,settlement_amount
        ,settlement_irf_amount
        ,itx_jurisdiction interchange_jurisdiction
        ,itx_business_mode interchange_business_mode
        ,itx_fee_descriptor interchange_fee_descriptor
        ,interchange_business_transaction_type
        ,interchange_reversal_flag
        ,itx_count interchange_count
        ,itx_amount interchange_amount
        ,itx_irf_amount interchange_irf_amount
        ,diff_count
        ,diff_amount
        ,diff_irf_amount
        ,diff_count_abs
        ,metric_count
        ,metric_excess_count
        ,absolute_error
        from 
        temporal.{customer_code}_{type_file}_{string_date}_mastercard_validation_error_sett_interchange
        """
        table_scheme_tmp = "temporal"
        table_name_tmp = f"{customer_code}_{type_file}_{string_date}_mastercard_validation_interchange"
        table_tmp = f"{table_scheme_tmp}.{table_name_tmp}"
        message_drop = self.ps.drop_table(table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_drop,
            self.module,
        )
        message_create = self.ps.create_table_from_select(query_tmp, table_tmp)
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE",
            "INFO",
            message_create,
            self.module,
        )

        message_insert = self.ps.insert_from_table(
            table_scheme_tmp, table_name_tmp, table_scheme, table_name
        )
        log.logs().exist_file(
            "OPERATIONAL",
            customer_code,
            "MASTERCARD",
            self.log_name,
            "VALIDATION OF MASTERCARD INTERCHANGE FILE",
            "INFO",
            message_insert,
            self.module,
        )

        return "finished"
