-- DROP FUNCTION analytics.get_mastercard_transactions(text, date, int4);

CREATE OR REPLACE FUNCTION analytics.get_mastercard_transactions(param_customer_code text, param_processing_date date, param_row_limit integer DEFAULT NULL::integer)
 RETURNS TABLE(customer_code text, file_id text, row_id integer, customer_country_code text, business_mode_code text, brand_code text, processing_date date, processing_month text, transaction_date date, transaction_month text, transaction_group_code text, transaction_type_id integer, jurisdiction_code text, merchant_country_code text, mcc_code integer, merchant text, merchant_id text, terminal_id text, acquirer_bin integer, issuer_country_code text, issuer_bin_6 integer, issuer_bin_8 integer, funding_source_code text, product_program_id integer, product_code text, card_present_code text, is_reversal_or_chargeback boolean, interchange_rule text, reported_currency_code text, transaction_amount numeric, interchange_fees_amount numeric, scheme_fees_amount numeric)
 LANGUAGE plpgsql
AS $function$

BEGIN

	DECLARE	
			duplicate_on_us_mc_flg BOOLEAN := FALSE;
	BEGIN 
		
	   	SELECT duplicate_on_us_flag_mastercard
	    INTO duplicate_on_us_mc_flg
	    FROM control.t_customer
	    WHERE code = PARAM_CUSTOMER_CODE;		
		
		SET ENABLE_PARTITIONWISE_JOIN TO ON;
	
	    DROP TABLE IF EXISTS tmp_table_to_return;

	   	IF duplicate_on_us_mc_flg IS FALSE THEN
	   	
		    CREATE TEMPORARY TABLE tmp_table_to_return AS
		    SELECT T1.app_customer_code                                                       customer_code,
		           T1.app_hash_file                                                           file_id,
		           T1.app_id::INT                                                             row_id,
		           CASE
		               WHEN T1.app_type_file = 'IN' AND T1.app_message_type <> '1442'
		                   THEN M2.country_code_alternative
		               WHEN T1.app_type_file = 'IN' AND T1.app_message_type = '1442'
		                   THEN M1.country_code_alternative
		               WHEN T1.app_type_file = 'OUT' AND T1.app_message_type <> '1442'
		                   THEN M1.country_code_alternative
		               WHEN T1.app_type_file = 'OUT' AND T1.app_message_type = '1442'
		                   THEN M2.country_code_alternative
		               END::TEXT                                                              customer_country_code,
		           CASE
		               WHEN T1.app_type_file = 'IN' THEN 'I'
		               WHEN T1.app_type_file = 'OUT' THEN 'A'
		               END::TEXT                                                              business_mode_code,
		           'MC'                                                                       brand_code,
		           T1.app_processing_date                                                     processing_date,
		           LEFT(T1.app_processing_date::TEXT, 7)                                      processing_month,
		           T1.date_and_time_local_transaction::DATE                                   transaction_date,
		           LEFT(T1.date_and_time_local_transaction::DATE::TEXT, 7)                    transaction_month,
		           CASE
		               WHEN LEFT(T1.processing_code, 2) IN ('00', '09', '18') THEN 'PUR'
		               WHEN LEFT(T1.processing_code, 2) IN ('20') THEN 'CRD'
		               WHEN LEFT(T1.processing_code, 2) IN ('01', '12', '17', '28', '50') THEN 'CSH'
		               ELSE 'OTH'
		               END                                                                    transaction_group_code,
		           LEFT(T1.processing_code, 2)::INT                                           transaction_type_id,
		           T2.jurisdiction::TEXT                                                      jurisdiction_code,
		           CASE
		               WHEN T1.app_message_type <> '1442' THEN M1.country_code_alternative
		               WHEN T1.app_message_type = '1442' THEN M2.country_code_alternative
		               END::TEXT                                                              merchant_country_code,
		           T1.card_acceptor_business_code_mcc                                         mcc_code,
		           TRIM(UPPER(T1.card_acceptor_name))::TEXT                                   merchant,
		           TRIM(UPPER(T1.card_acceptor_id_code))::TEXT                                merchant_id,
		           TRIM(UPPER(T1.card_acceptor_terminal_id))::TEXT                            terminal_id,
		           CASE
		               WHEN TRIM(T1.acquirer_reference_data) = '' THEN NULL::INT
		               ELSE SUBSTRING(T1.acquirer_reference_data, 2, 6)::INT
		               END                                                                    acquirer_bin,
		           CASE
		               WHEN T1.app_message_type <> '1442' THEN M2.country_code_alternative
		               WHEN T1.app_message_type = '1442' THEN M1.country_code_alternative
		               END::TEXT                                                              issuer_country_code,
		           LEFT(T1.pan::TEXT, 6)::INT                                                 issuer_bin_6,
		           LEFT(T1.pan::TEXT, 8)::INT                                                 issuer_bin_8,
		           T2.funding_source::TEXT                                                    funding_source_code,
		           M5.range_program_id::INT                                                   product_program_id,
		           COALESCE(T1.licensed_product_identifier, T2.gcms_product_identifier)::TEXT product_code,
		           CASE
		               WHEN SUBSTRING(T1.pos_entry_mode, 6, 1) = '1' THEN 'CPR'
		               WHEN SUBSTRING(T1.pos_entry_mode, 6, 1) = '0' THEN 'CNP'
		               ELSE 'UNK'
		               END                                                                    card_present_code,
		           CASE
		               WHEN (T1.app_message_type = '1240' AND COALESCE(T1.message_reversal_indicator, '') <> '')
		                   OR (T1.app_message_type = '1442' AND COALESCE(T1.message_reversal_indicator, '') = '')
		                   THEN TRUE
		               ELSE FALSE
		               END                                                                    is_reversal_or_chargeback,
		           T2.jurisdiction_assigned || '-' || T3.ird::TEXT                            interchange_rule,
		           M6.report_currency_code::TEXT                                              reported_currency_code,
		           COALESCE(X1.exchange_value, 1) * T1.amount_transaction                     transaction_amount,
		           COALESCE(X2.exchange_value, X1.exchange_value, 1) * T3.calculated_value    interchange_fees_amount,
		           S1.unitary_scheme_fee_cost::NUMERIC             							  scheme_fees_amount
		    FROM operational.dh_mastercard_data_element T1
		             LEFT JOIN operational.m_country M1
		                       ON M1.country_code_alternative = T1.card_acceptor_country_code
		             LEFT JOIN operational.dh_mastercard_calculated_field T2
		                       ON T2.app_customer_code = T1.app_customer_code
		                           AND T2.app_type_file = T1.app_type_file
		                           AND T2.app_processing_date = T1.app_processing_date
		                           AND T2.app_hash_file = T1.app_hash_file
		                           AND T2.app_id = T1.app_id
		             LEFT JOIN operational.m_country M2 ON M2.country_code_alternative = T2.iar_country
		             LEFT JOIN operational.dh_mastercard_interchange T3
		                       ON T3.app_customer_code = T1.app_customer_code
		                           AND T3.app_type_file = T1.app_type_file
		                           AND T3.app_processing_date = T1.app_processing_date
		                           AND T3.app_hash_file = T1.app_hash_file
		                           AND T3.app_id = T1.app_id
		             LEFT JOIN operational.m_mastercard_bin_products M5
		                       ON M5.bin_product_id = COALESCE(T1.licensed_product_identifier, T2.gcms_product_identifier)
		             LEFT JOIN control.t_customer M6 ON M6.code = T1.app_customer_code
		             LEFT JOIN operational.dh_exchange_rate X1
		                       ON X1.app_processing_date = T1.app_processing_date
		                           AND X1.brand = 'MasterCard'
		                           AND X1.currency_from_code = T1.currency_code_transaction::TEXT
		                           AND X1.currency_to = M6.report_currency_code
		             LEFT JOIN operational.dh_exchange_rate X2
		                       ON X2.app_processing_date = T3.app_processing_date
		                           AND X2.brand = 'MasterCard'
		                           AND X2.currency_from = T3.rate_currency
		                           AND X2.currency_to = M6.report_currency_code
		             LEFT JOIN operational.mh_transaction_scheme_fee S1
		                   	   ON S1.app_customer_code = T1.app_customer_code
			                   	   AND S1.app_type_file = T1.app_type_file
			                   	   AND S1.app_processing_date = T1.app_processing_date
		             			   AND S1.app_hash_file = T1.app_hash_file
			                   	   AND S1.app_id = T1.app_id 
		             			   AND S1.transaction_brand = 'MasterCard'                       
		    WHERE T1.app_customer_code = PARAM_CUSTOMER_CODE
		      AND T1.app_type_file IN ('IN', 'OUT')
		      AND T1.app_processing_date = PARAM_PROCESSING_DATE
		      AND T1.app_message_type IN ('1240', '1442')
		    LIMIT PARAM_ROW_LIMIT;

	   	ELSE 
	   	
		    CREATE TEMPORARY TABLE tmp_table_to_return AS
		    SELECT T1.app_customer_code                                                       customer_code,
		           T1.app_hash_file                                                           file_id,
		           T1.app_id::INT                                                             row_id,
		           CASE
		               WHEN T1.app_type_file = 'IN' AND T1.app_message_type <> '1442'
		                   THEN M2.country_code_alternative
		               WHEN T1.app_type_file = 'IN' AND T1.app_message_type = '1442'
		                   THEN M1.country_code_alternative
		               WHEN T1.app_type_file = 'OUT' AND T1.app_message_type <> '1442'
		                   THEN M1.country_code_alternative
		               WHEN T1.app_type_file = 'OUT' AND T1.app_message_type = '1442'
		                   THEN M2.country_code_alternative
		               END::TEXT                                                              customer_country_code,
		           CASE
		               WHEN T1.app_type_file = 'IN' THEN 'I'
		               WHEN T1.app_type_file = 'OUT' THEN 'A'
		               END::TEXT                                                              business_mode_code,
		           'MC'                                                                       brand_code,
		           T1.app_processing_date                                                     processing_date,
		           LEFT(T1.app_processing_date::TEXT, 7)                                      processing_month,
		           T1.date_and_time_local_transaction::DATE                                   transaction_date,
		           LEFT(T1.date_and_time_local_transaction::DATE::TEXT, 7)                    transaction_month,
		           CASE
		               WHEN LEFT(T1.processing_code, 2) IN ('00', '09', '18') THEN 'PUR'
		               WHEN LEFT(T1.processing_code, 2) IN ('20') THEN 'CRD'
		               WHEN LEFT(T1.processing_code, 2) IN ('01', '12', '17', '28', '50') THEN 'CSH'
		               ELSE 'OTH'
		               END                                                                    transaction_group_code,
		           LEFT(T1.processing_code, 2)::INT                                           transaction_type_id,
		           T2.jurisdiction::TEXT                                                      jurisdiction_code,
		           CASE
		               WHEN T1.app_message_type <> '1442' THEN M1.country_code_alternative
		               WHEN T1.app_message_type = '1442' THEN M2.country_code_alternative
		               END::TEXT                                                              merchant_country_code,
		           T1.card_acceptor_business_code_mcc                                         mcc_code,
		           TRIM(UPPER(T1.card_acceptor_name))::TEXT                                   merchant,
		           TRIM(UPPER(T1.card_acceptor_id_code))::TEXT                                merchant_id,
		           TRIM(UPPER(T1.card_acceptor_terminal_id))::TEXT                            terminal_id,
		           CASE
		               WHEN TRIM(T1.acquirer_reference_data) = '' THEN NULL::INT
		               ELSE SUBSTRING(T1.acquirer_reference_data, 2, 6)::INT
		               END                                                                    acquirer_bin,
		           CASE
		               WHEN T1.app_message_type <> '1442' THEN M2.country_code_alternative
		               WHEN T1.app_message_type = '1442' THEN M1.country_code_alternative
		               END::TEXT                                                              issuer_country_code,
		           LEFT(T1.pan::TEXT, 6)::INT                                                 issuer_bin_6,
		           LEFT(T1.pan::TEXT, 8)::INT                                                 issuer_bin_8,
		           T2.funding_source::TEXT                                                    funding_source_code,
		           M5.range_program_id::INT                                                   product_program_id,
		           COALESCE(T1.licensed_product_identifier, T2.gcms_product_identifier)::TEXT product_code,
		           CASE
		               WHEN SUBSTRING(T1.pos_entry_mode, 6, 1) = '1' THEN 'CPR'
		               WHEN SUBSTRING(T1.pos_entry_mode, 6, 1) = '0' THEN 'CNP'
		               ELSE 'UNK'
		               END                                                                    card_present_code,
		           CASE
		               WHEN (T1.app_message_type = '1240' AND COALESCE(T1.message_reversal_indicator, '') <> '')
		                   OR (T1.app_message_type = '1442' AND COALESCE(T1.message_reversal_indicator, '') = '')
		                   THEN TRUE
		               ELSE FALSE
		               END                                                                    is_reversal_or_chargeback,
		           T2.jurisdiction_assigned || '-' || T3.ird::TEXT                            interchange_rule,
		           M6.report_currency_code::TEXT                                              reported_currency_code,
		           COALESCE(X1.exchange_value, 1) * T1.amount_transaction                     transaction_amount,
		           COALESCE(X2.exchange_value, X1.exchange_value, 1) * T3.calculated_value    interchange_fees_amount,
		           S1.unitary_scheme_fee_cost::NUMERIC             							  scheme_fees_amount
		    FROM operational.dh_mastercard_data_element T1
		             LEFT JOIN operational.m_country M1
		                       ON M1.country_code_alternative = T1.card_acceptor_country_code
		             LEFT JOIN operational.dh_mastercard_calculated_field T2
		                       ON T2.app_customer_code = T1.app_customer_code
		                           AND T2.app_type_file = T1.app_type_file
		                           AND T2.app_processing_date = T1.app_processing_date
		                           AND T2.app_hash_file = T1.app_hash_file
		                           AND T2.app_id = T1.app_id
		             LEFT JOIN operational.m_country M2 ON M2.country_code_alternative = T2.iar_country
		             LEFT JOIN operational.dh_mastercard_interchange T3
		                       ON T3.app_customer_code = T1.app_customer_code
		                           AND T3.app_type_file = T1.app_type_file
		                           AND T3.app_processing_date = T1.app_processing_date
		                           AND T3.app_hash_file = T1.app_hash_file
		                           AND T3.app_id = T1.app_id
		             LEFT JOIN operational.m_mastercard_bin_products M5
		                       ON M5.bin_product_id = COALESCE(T1.licensed_product_identifier, T2.gcms_product_identifier)
		             LEFT JOIN control.t_customer M6 ON M6.code = T1.app_customer_code
		             LEFT JOIN operational.dh_exchange_rate X1
		                       ON X1.app_processing_date = T1.app_processing_date
		                           AND X1.brand = 'MasterCard'
		                           AND X1.currency_from_code = T1.currency_code_transaction::TEXT
		                           AND X1.currency_to = M6.report_currency_code
		             LEFT JOIN operational.dh_exchange_rate X2
		                       ON X2.app_processing_date = T3.app_processing_date
		                           AND X2.brand = 'MasterCard'
		                           AND X2.currency_from = T3.rate_currency
		                           AND X2.currency_to = M6.report_currency_code
		             LEFT JOIN operational.mh_transaction_scheme_fee S1
		                   	   ON S1.app_customer_code = T1.app_customer_code
			                   	   AND S1.app_type_file = T1.app_type_file
			                   	   AND S1.app_processing_date = T1.app_processing_date
		             			   AND S1.app_hash_file = T1.app_hash_file
			                   	   AND S1.app_id = T1.app_id 
		             			   AND S1.transaction_brand = 'MasterCard'                       
		    WHERE T1.app_customer_code = PARAM_CUSTOMER_CODE
		      AND T1.app_type_file IN ('IN', 'OUT')
		      AND T1.app_processing_date = PARAM_PROCESSING_DATE
		      AND T1.app_message_type IN ('1240', '1442')
		      and S1.table_description <> 'MASTERCARD ON-US DUP (ACQ TO ISS)'
		    LIMIT PARAM_ROW_LIMIT;
		   
		   	DROP TABLE IF EXISTS tmp_table_dup;
		   
		   	CREATE TEMPORARY TABLE tmp_table_dup AS 
		   	SELECT *
		   	FROM tmp_table_to_return t1
		   	WHERE t1.jurisdiction_code ='on-us'	AND t1.business_mode_code='A';
		   
		   	INSERT INTO tmp_table_to_return
		   	SELECT T1.customer_code,
		   		   T1.file_id,
		   		   T1.row_id,
		   		   T1.customer_country_code,
		   		   'I' business_mode_code,
		   		   T1.brand_code,
		   		   T1.processing_date,
		   		   T1.processing_month,
		   		   T1.transaction_date,
		   		   T1.transaction_month,
		   		   T1.transaction_group_code,
		   		   T1.transaction_type_id,
		   		   T1.jurisdiction_code,
		   		   T1.merchant_country_code,
		   		   T1.mcc_code,
		   		   T1.merchant,        
		   		   T1.merchant_id,    
		   		   T1.terminal_id,
		   		   T1.acquirer_bin,
		   		   T1.issuer_country_code,
		   		   T1.issuer_bin_6,
		   		   T1.issuer_bin_8,
		   		   T1.funding_source_code,
		   		   T1.product_program_id,
		   		   T1.product_code,
		   		   T1.card_present_code,
		   		   T1.is_reversal_or_chargeback,
		   		   T1.interchange_rule,
		   		   T1.reported_currency_code,
		   		   T1.transaction_amount,  
		   		   T1.interchange_fees_amount,
		   		   T2.unitary_scheme_fee_cost::NUMERIC scheme_fees_amount
		   	FROM tmp_table_dup T1
		   		LEFT JOIN operational.mh_transaction_scheme_fee T2 ON T2.app_customer_code= T1.customer_code
		       	   AND T2.app_type_file = 'OUT'
		       	   AND T2.app_processing_date = T1.processing_date
				   AND T2.app_hash_file = T1.file_id
		       	   AND T2.app_id = T1.row_id 
				   AND T2.transaction_brand = 'MasterCard'	
				   AND T2.table_description = 'MASTERCARD ON-US DUP (ACQ TO ISS)';
		   	
	   	END IF;
	   	
		UPDATE tmp_table_to_return T1
		SET transaction_amount      = T1.transaction_amount * -1,
		    interchange_fees_amount = T1.interchange_fees_amount * -1
		WHERE T1.is_reversal_or_chargeback IS TRUE;
		
		RETURN QUERY
		    SELECT *
		    FROM tmp_table_to_return;
   END;

END;

$function$
;


-- DROP FUNCTION analytics.get_visa_baseii_transactions(text, date, int4);

CREATE OR REPLACE FUNCTION analytics.get_visa_baseii_transactions(param_customer_code text, param_processing_date date, param_row_limit integer DEFAULT NULL::integer)
 RETURNS TABLE(customer_code text, file_id text, row_id integer, customer_country_code text, business_mode_code text, brand_code text, processing_date date, processing_month text, transaction_date date, transaction_month text, transaction_group_code text, transaction_type_id integer, jurisdiction_code text, merchant_country_code text, mcc_code integer, merchant text, merchant_id text, terminal_id text, acquirer_bin integer, issuer_country_code text, issuer_bin_6 integer, issuer_bin_8 integer, funding_source_code text, product_program_id integer, product_code text, card_present_code text, is_reversal_or_chargeback boolean, interchange_rule text, reported_currency_code text, transaction_amount numeric, interchange_fees_amount numeric, scheme_fees_amount numeric)
 LANGUAGE plpgsql
AS $function$

BEGIN
	
	DECLARE	
			duplicate_on_us_visa_flg BOOLEAN := FALSE;
	BEGIN 
	    
	   	SELECT duplicate_on_us_flag_visa
	    INTO duplicate_on_us_visa_flg
	    FROM control.t_customer
	    WHERE code = PARAM_CUSTOMER_CODE;	   
	   
	    SET ENABLE_PARTITIONWISE_JOIN TO ON;
	
	    DROP TABLE IF EXISTS tmp_table_to_return;
	    	   
	   	IF duplicate_on_us_visa_flg IS FALSE THEN
	
		    CREATE TEMPORARY TABLE tmp_table_to_return AS
		    SELECT T1.app_customer_code                                                    customer_code,
		           T1.app_hash_file                                                        file_id,
		           T1.app_id::INT                                                          row_id,
		           CASE
		               WHEN T1.app_type_file = 'IN' THEN M2.country_code_alternative
		               WHEN T1.app_type_file = 'OUT' THEN M1.country_code_alternative
		               END::TEXT                                                           customer_country_code,
		           CASE
		               WHEN T1.app_type_file = 'IN' THEN 'I'
		               WHEN T1.app_type_file = 'OUT' THEN 'A'
		               END                                                                 business_mode_code,
		           'VI'                                                                    brand_code,
		           T1.app_processing_date                                                  processing_date,
		           LEFT(T1.app_processing_date::TEXT, 7)                                   processing_month,
		           T1.purchase_date                                                        transaction_date,
		           LEFT(T1.purchase_date::TEXT, 7)                                         transaction_month,
		           CASE
		               WHEN T1.transaction_code IN ('05', '25') THEN 'PUR'
		               WHEN T1.transaction_code IN ('06', '26') THEN 'CRD'
		               WHEN T1.transaction_code IN ('07', '27') THEN 'CSH'
		               ELSE 'OTH'
		               END                                                                 transaction_group_code,
		           T2.business_transaction_type                                            transaction_type_id,
		           T2.jurisdiction::TEXT                                                   jurisdiction_code,
		           M1.country_code_alternative::TEXT                                       merchant_country_code,
		           T1.merchant_category_code                                               mcc_code,
		           TRIM(UPPER(T1.merchant_name))::TEXT                                     merchant,
		           TRIM(UPPER(T1.card_acceptor_id))::TEXT                                  merchant_id,
		           TRIM(UPPER(T1.terminal_id))::TEXT                                       terminal_id,
		           T1.account_reference_number_acquiring_identifier                        acquirer_bin,
		           M2.country_code_alternative::TEXT                                       issuer_country_code,
		           LEFT(T1.account_number::TEXT, 6)::INT                                   issuer_bin_6,
		           LEFT(T1.account_number::TEXT, 8)::INT                                   issuer_bin_8,
		           T2.funding_source::TEXT                                                 funding_source_code,
		           M5.range_program_id::INT                                                product_program_id,
		           T2.product_id                                                           product_code,
		           CASE
		               WHEN T1.motoec_indicator = ' ' THEN 'CPR'
		               WHEN REPLACE(T1.motoec_indicator, ' ', '0')::INT BETWEEN 1 AND 9 THEN 'CNP'
		               ELSE 'UNK'
		               END                                                                 card_present_code,
		           T2.reversal_indicator::BOOLEAN                                          is_reversal_or_chargeback,
		           M4.fee_descriptor::TEXT                                                 interchange_rule,
		           M6.report_currency_code::TEXT                                           reported_currency_code,
		           COALESCE(X1.exchange_value, 1) * T1.source_amount                       transaction_amount,
		           COALESCE(X2.exchange_value, X1.exchange_value, 1) * T3.calculated_value interchange_fees_amount,
		           S1.unitary_scheme_fee_cost::NUMERIC                                     scheme_fees_amount           
		
		    FROM operational.dh_visa_transaction T1
		             LEFT JOIN operational.m_country M1 ON M1.country_code = T1.merchant_country_code
		             LEFT JOIN operational.dh_visa_transaction_calculated_field T2
		                       ON T2.app_customer_code = T1.app_customer_code
		                           AND T2.app_type_file = T1.app_type_file
		                           AND T2.app_processing_date = T1.app_processing_date
		                           AND T2.app_hash_file = T1.app_hash_file
		                           AND T2.app_id = T1.app_id
		             LEFT JOIN operational.m_country M2 ON M2.country_code = T2.ardef_country
		             LEFT JOIN operational.dh_visa_interchange T3
		                       ON T3.app_customer_code = T1.app_customer_code
		                           AND T3.app_type_file = T1.app_type_file
		                           AND T3.app_processing_date = T1.app_processing_date
		                           AND T3.app_hash_file = T1.app_hash_file
		                           AND T3.app_id = T1.app_id
		             LEFT JOIN operational.m_interchange_rules_visa M4
		                       ON M4.region_country_code = T3.region_country_code
		                           AND T3.app_processing_date BETWEEN M4.valid_from AND COALESCE(M4.valid_until, '9999-12-31')
		                           AND M4.intelica_id = T3.intelica_id::TEXT
		             LEFT JOIN operational.m_visa_bin_products M5 ON M5.bin_product_id = T2.product_id
		             LEFT JOIN control.t_customer M6 ON M6.code = T1.app_customer_code
		             LEFT JOIN operational.dh_exchange_rate X1
		                       ON X1.app_processing_date = T1.app_processing_date
		                           AND X1.brand = 'VISA'
		                           AND X1.currency_from_code::INT = T1.source_currency_code::INT
		                           AND X1.currency_to = M6.report_currency_code
		             LEFT JOIN operational.dh_exchange_rate X2
		                       ON X2.app_processing_date = T3.app_processing_date
		                           AND X2.brand = 'VISA'
		                           AND X2.currency_from = T3.fee_currency
		                           AND X2.currency_to = M6.report_currency_code
		            LEFT JOIN operational.mh_transaction_scheme_fee S1
		                   	   ON S1.app_customer_code=t1.app_customer_code
			                   	   AND S1.app_type_file = T1.app_type_file
			                   	   AND S1.app_processing_date = T1.app_processing_date
		             			   AND S1.app_hash_file = T1.app_hash_file
			                   	   AND S1.app_id = T1.app_id 
		             			   AND S1.transaction_brand = 'VISA'	                           
		    WHERE T1.app_customer_code = PARAM_CUSTOMER_CODE
		      AND T1.app_type_file IN ('IN', 'OUT')
		      AND T1.app_processing_date = PARAM_PROCESSING_DATE
		    LIMIT PARAM_ROW_LIMIT;
		   
	   	ELSE 
	   	
	   	 	CREATE TEMPORARY TABLE tmp_table_to_return AS
		    SELECT T1.app_customer_code                                                    customer_code,
		           T1.app_hash_file                                                        file_id,
		           T1.app_id::INT                                                          row_id,
		           CASE
		               WHEN T1.app_type_file = 'IN' THEN M2.country_code_alternative
		               WHEN T1.app_type_file = 'OUT' THEN M1.country_code_alternative
		               END::TEXT                                                           customer_country_code,
		           CASE
		               WHEN T1.app_type_file = 'IN' THEN 'I'
		               WHEN T1.app_type_file = 'OUT' THEN 'A'
		               END                                                                 business_mode_code,
		           'VI'                                                                    brand_code,
		           T1.app_processing_date                                                  processing_date,
		           LEFT(T1.app_processing_date::TEXT, 7)                                   processing_month,
		           T1.purchase_date                                                        transaction_date,
		           LEFT(T1.purchase_date::TEXT, 7)                                         transaction_month,
		           CASE
		               WHEN T1.transaction_code IN ('05', '25') THEN 'PUR'
		               WHEN T1.transaction_code IN ('06', '26') THEN 'CRD'
		               WHEN T1.transaction_code IN ('07', '27') THEN 'CSH'
		               ELSE 'OTH'
		               END                                                                 transaction_group_code,
		           T2.business_transaction_type                                            transaction_type_id,
		           T2.jurisdiction::TEXT                                                   jurisdiction_code,
		           M1.country_code_alternative::TEXT                                       merchant_country_code,
		           T1.merchant_category_code                                               mcc_code,
		           TRIM(UPPER(T1.merchant_name))::TEXT                                     merchant,
		           TRIM(UPPER(T1.card_acceptor_id))::TEXT                                  merchant_id,
		           TRIM(UPPER(T1.terminal_id))::TEXT                                       terminal_id,
		           T1.account_reference_number_acquiring_identifier                        acquirer_bin,
		           M2.country_code_alternative::TEXT                                       issuer_country_code,
		           LEFT(T1.account_number::TEXT, 6)::INT                                   issuer_bin_6,
		           LEFT(T1.account_number::TEXT, 8)::INT                                   issuer_bin_8,
		           T2.funding_source::TEXT                                                 funding_source_code,
		           M5.range_program_id::INT                                                product_program_id,
		           T2.product_id                                                           product_code,
		           CASE
		               WHEN T1.motoec_indicator = ' ' THEN 'CPR'
		               WHEN REPLACE(T1.motoec_indicator, ' ', '0')::INT BETWEEN 1 AND 9 THEN 'CNP'
		               ELSE 'UNK'
		               END                                                                 card_present_code,
		           T2.reversal_indicator::BOOLEAN                                          is_reversal_or_chargeback,
		           M4.fee_descriptor::TEXT                                                 interchange_rule,
		           M6.report_currency_code::TEXT                                           reported_currency_code,
		           COALESCE(X1.exchange_value, 1) * T1.source_amount                       transaction_amount,
		           COALESCE(X2.exchange_value, X1.exchange_value, 1) * T3.calculated_value interchange_fees_amount,
		           S1.unitary_scheme_fee_cost::NUMERIC                                     scheme_fees_amount  
		    FROM operational.dh_visa_transaction T1
		             LEFT JOIN operational.m_country M1 ON M1.country_code = T1.merchant_country_code
		             LEFT JOIN operational.dh_visa_transaction_calculated_field T2
		                       ON T2.app_customer_code = T1.app_customer_code
		                           AND T2.app_type_file = T1.app_type_file
		                           AND T2.app_processing_date = T1.app_processing_date
		                           AND T2.app_hash_file = T1.app_hash_file
		                           AND T2.app_id = T1.app_id
		             LEFT JOIN operational.m_country M2 ON M2.country_code = T2.ardef_country
		             LEFT JOIN operational.dh_visa_interchange T3
		                       ON T3.app_customer_code = T1.app_customer_code
		                           AND T3.app_type_file = T1.app_type_file
		                           AND T3.app_processing_date = T1.app_processing_date
		                           AND T3.app_hash_file = T1.app_hash_file
		                           AND T3.app_id = T1.app_id
		             LEFT JOIN operational.m_interchange_rules_visa M4
		                       ON M4.region_country_code = T3.region_country_code
		                           AND T3.app_processing_date BETWEEN M4.valid_from AND COALESCE(M4.valid_until, '9999-12-31')
		                           AND M4.intelica_id = T3.intelica_id::TEXT
		             LEFT JOIN operational.m_visa_bin_products M5 ON M5.bin_product_id = T2.product_id
		             LEFT JOIN control.t_customer M6 ON M6.code = T1.app_customer_code
		             LEFT JOIN operational.dh_exchange_rate X1
		                       ON X1.app_processing_date = T1.app_processing_date
		                           AND X1.brand = 'VISA'
		                           AND X1.currency_from_code::INT = T1.source_currency_code::INT
		                           AND X1.currency_to = M6.report_currency_code
		             LEFT JOIN operational.dh_exchange_rate X2
		                       ON X2.app_processing_date = T3.app_processing_date
		                           AND X2.brand = 'VISA'
		                           AND X2.currency_from = T3.fee_currency
		                           AND X2.currency_to = M6.report_currency_code
					 LEFT JOIN operational.mh_transaction_scheme_fee S1
	               	 	  	   ON S1.app_customer_code=t1.app_customer_code
			                   	   AND S1.app_type_file = T1.app_type_file
			                   	   AND S1.app_processing_date = T1.app_processing_date
		             			   AND S1.app_hash_file = T1.app_hash_file
			                   	   AND S1.app_id = T1.app_id 
		             			   AND S1.transaction_brand = 'VISA'		             			   
		    WHERE T1.app_customer_code = PARAM_CUSTOMER_CODE
			  AND T1.app_type_file IN ('IN', 'OUT')
			  AND T1.app_processing_date = PARAM_PROCESSING_DATE
			  AND S1.table_description <> 'VISA ON-US DUP (ACQ TO ISS)'
		    LIMIT PARAM_ROW_LIMIT;	   	 	  
		  
			DROP TABLE IF EXISTS tmp_table_dup;
		   
		   	CREATE TEMPORARY TABLE tmp_table_dup AS 
		   	SELECT *
		   	FROM tmp_table_to_return t1
		   	WHERE t1.jurisdiction_code ='on-us'	AND t1.business_mode_code='A';
		   
		   	INSERT INTO tmp_table_to_return
		   	SELECT T1.customer_code,
		   		   T1.file_id,
		   		   T1.row_id,
		   		   T1.customer_country_code,
		   		   'I' business_mode_code,
		   		   T1.brand_code,
		   		   T1.processing_date,
		   		   T1.processing_month,
		   		   T1.transaction_date,
		   		   T1.transaction_month,
		   		   T1.transaction_group_code,
		   		   T1.transaction_type_id,
		   		   T1.jurisdiction_code,
		   		   T1.merchant_country_code,
		   		   T1.mcc_code,
		   		   T1.merchant,        
		   		   T1.merchant_id,    
		   		   T1.terminal_id,
		   		   T1.acquirer_bin,
		   		   T1.issuer_country_code,
		   		   T1.issuer_bin_6,
		   		   T1.issuer_bin_8,
		   		   T1.funding_source_code,
		   		   T1.product_program_id,
		   		   T1.product_code,
		   		   T1.card_present_code,
		   		   T1.is_reversal_or_chargeback,
		   		   T1.interchange_rule,
		   		   T1.reported_currency_code,
		   		   T1.transaction_amount,  
		   		   T1.interchange_fees_amount,
		   		   T2.unitary_scheme_fee_cost::NUMERIC scheme_fees_amount
		   	FROM tmp_table_dup T1
		   		LEFT JOIN operational.mh_transaction_scheme_fee T2 ON T2.app_customer_code= T1.customer_code
		       	   AND T2.app_type_file = 'OUT'
		       	   AND T2.app_processing_date = T1.processing_date
				   AND T2.app_hash_file = T1.file_id
		       	   AND T2.app_id = T1.row_id 
				   AND T2.transaction_brand = 'VISA'	
				   AND T2.table_description = 'VISA ON-US DUP (ACQ TO ISS)';
		   
	   	END IF;
	   	
		UPDATE tmp_table_to_return T1
		SET transaction_amount      = T1.transaction_amount * -1,
		    interchange_fees_amount = T1.interchange_fees_amount * -1
		WHERE T1.is_reversal_or_chargeback IS TRUE;
		
		RETURN QUERY
		    SELECT *
		    FROM tmp_table_to_return;
   END;

END;

$function$
;

-- DROP FUNCTION analytics.get_visa_sms_transactions(text, date, int4);

CREATE OR REPLACE FUNCTION analytics.get_visa_sms_transactions(param_customer_code text, param_processing_date date, param_row_limit integer DEFAULT NULL::integer)
 RETURNS TABLE(customer_code text, file_id text, row_id integer, customer_country_code text, business_mode_code text, brand_code text, processing_date date, processing_month text, transaction_date date, transaction_month text, transaction_group_code text, transaction_type_id integer, jurisdiction_code text, merchant_country_code text, mcc_code integer, merchant text, merchant_id text, terminal_id text, acquirer_bin integer, issuer_country_code text, issuer_bin_6 integer, issuer_bin_8 integer, funding_source_code text, product_program_id integer, product_code text, card_present_code text, is_reversal_or_chargeback boolean, interchange_rule text, reported_currency_code text, transaction_amount numeric, interchange_fees_amount numeric, scheme_fees_amount numeric)
 LANGUAGE plpgsql
AS $function$

BEGIN

	DECLARE	
			duplicate_on_us_visa_flg BOOLEAN := FALSE;
	BEGIN 
	    
	   	SELECT duplicate_on_us_flag_visa
	    INTO duplicate_on_us_visa_flg
	    FROM control.t_customer
	    WHERE code = PARAM_CUSTOMER_CODE;	   
	   
	    SET ENABLE_PARTITIONWISE_JOIN TO ON;
	
	    DROP TABLE IF EXISTS tmp_table_to_return;
	    	   
	   	IF duplicate_on_us_visa_flg IS FALSE THEN

		    CREATE TEMPORARY TABLE tmp_table_to_return AS
		    SELECT T1.app_customer_code                            customer_code,
		           T1.app_hash_file                                file_id,
		           T1.app_id::INT                                  row_id,
		           CASE
		               WHEN T1.issuer_acquirer_indicator = 'I' THEN M2.country_code_alternative
		               WHEN T1.issuer_acquirer_indicator = 'A' THEN M1.country_code_alternative
		               END::TEXT                                   customer_country_code,
		           T1.issuer_acquirer_indicator::TEXT              business_mode_code,
		           'VI'                                            brand_code,
		           T1.app_processing_date                          processing_date,
		           LEFT(T1.app_processing_date::TEXT, 7)           processing_month,
		           T1.local_transaction_date                       transaction_date,
		           LEFT(T1.local_transaction_date::TEXT, 7)        transaction_month,
		           CASE
		               WHEN T2.transaction_code_sms IN ('05', '25') THEN 'PUR'
		               WHEN T2.transaction_code_sms IN ('06', '26') THEN 'CRD'
		               WHEN T2.transaction_code_sms IN ('07', '27') THEN 'CSH'
		               ELSE 'OTH'
		               END                                         transaction_group_code,
		           T2.business_transaction_type                    transaction_type_id,
		           T2.jurisdiction::TEXT                           jurisdiction_code,
		           M1.country_code_alternative::TEXT               merchant_country_code,
		           T1.merchants_type                               mcc_code,
		           TRIM(UPPER(T1.card_acceptor_name))::TEXT        merchant,
		           TRIM(UPPER(T1.card_acceptor_id_sms))::TEXT      merchant_id,
		           TRIM(UPPER(T1.card_acceptor_terminal_id))::TEXT terminal_id,
		           T1.acquiring_institution_id_1::INT              acquirer_bin,
		           M2.country_code_alternative::TEXT               issuer_country_code,
		           LEFT(T1.card_number::TEXT, 6)::INT              issuer_bin_6,
		           LEFT(T1.card_number::TEXT, 8)::INT              issuer_bin_8,
		           T2.funding_source::TEXT                         funding_source_code,
		           M5.range_program_id::INT                        product_program_id,
		           T2.product_id                                   product_code,
		           CASE
		               WHEN T1.mailtelephone_or_electronic_commerce_indicator = ' ' THEN 'CPR'
		               WHEN REPLACE(T1.mailtelephone_or_electronic_commerce_indicator, ' ', '0')::INT BETWEEN 1 AND 9
		                   THEN 'CNP'
		               ELSE 'UNK'
		               END                                         card_present_code,
		           T2.reversal_indicator::BOOLEAN                  is_reversal_or_chargeback,
		           M4.fee_descriptor::TEXT                         interchange_rule,
		           M6.report_currency_code::TEXT                   reported_currency_code,
		           CASE
		               WHEN T1.transaction_amount = 0
		                   THEN COALESCE(X3.exchange_value, 1) * (T1.cryptogram_amount + T1.surcharge_amount_sms)
		               ELSE COALESCE(X1.exchange_value, 1) * T1.transaction_amount
		               END                                         transaction_amount,
		           CASE
		               WHEN T1.transaction_amount = 0 AND
		                    (T3.fee_currency IS NULL OR T3.fee_currency <> M6.report_currency_code)
		                   THEN COALESCE(X3.exchange_value, 1) * T3.calculated_value
		               WHEN T1.transaction_amount = 0 AND T3.fee_currency = M6.report_currency_code
		                   THEN COALESCE(X2.exchange_value, 1) * T3.calculated_value
		               ELSE COALESCE(X2.exchange_value, X1.exchange_value, 1) * T3.calculated_value
		               END                                         interchange_fees_amount,
		           S1.unitary_scheme_fee_cost::NUMERIC             scheme_fees_amount
		    FROM operational.dh_visa_transaction_sms T1
		             LEFT JOIN operational.m_country M1 ON M1.country_code = T1.card_acceptor_country
		             LEFT JOIN operational.dh_visa_transaction_sms_calculated_field T2
		                       ON T2.app_customer_code = T1.app_customer_code
		                           AND T2.app_type_file = T1.app_type_file
		                           AND T2.app_processing_date = T1.app_processing_date
		                           AND T2.app_hash_file = T1.app_hash_file
		                           AND T2.app_id = T1.app_id
		             LEFT JOIN operational.m_country M2 ON M2.country_code = T2.ardef_country
		             LEFT JOIN operational.dh_visa_sms_interchange T3
		                       ON T3.app_customer_code = T1.app_customer_code
		                           AND T3.app_type_file = T1.app_type_file
		                           AND T3.app_processing_date = T1.app_processing_date
		                           AND T3.app_hash_file = T1.app_hash_file
		                           AND T3.app_id = T1.app_id
		             LEFT JOIN operational.m_interchange_rules_visa M4
		                       ON M4.region_country_code = T3.region_country_code
		                           AND T3.app_processing_date BETWEEN M4.valid_from AND COALESCE(M4.valid_until, '9999-12-31')
		                           AND M4.intelica_id = T3.intelica_id::TEXT
		             LEFT JOIN operational.m_visa_bin_products M5 ON M5.bin_product_id = T2.product_id
		             LEFT JOIN control.t_customer M6 ON M6.code = T1.app_customer_code
		             LEFT JOIN operational.dh_exchange_rate X1
		                       ON X1.app_processing_date = T1.app_processing_date
		                           AND X1.brand = 'VISA'
		                           AND X1.currency_from_code::INT = T1.transaction_currency_code::INT
		                           AND X1.currency_to = M6.report_currency_code
		             LEFT JOIN operational.dh_exchange_rate X2
		                       ON X2.app_processing_date = T3.app_processing_date
		                           AND X2.brand = 'VISA'
		                           AND X2.currency_from = T3.fee_currency
		                           AND X2.currency_to = M6.report_currency_code
		             LEFT JOIN operational.dh_exchange_rate X3
		                       ON X3.app_processing_date = T1.app_processing_date
		                           AND X3.brand = 'VISA'
		                           AND X3.currency_from_code = '840' -- USD; Special Case.
		                           AND X3.currency_to = M6.report_currency_code
		             LEFT JOIN operational.mh_transaction_scheme_fee S1
		                   	   ON S1.app_customer_code=t1.app_customer_code
			                   	   AND S1.app_type_file = T1.app_type_file
			                   	   AND S1.app_processing_date = T1.app_processing_date
		             			   AND S1.app_hash_file = T1.app_hash_file
			                   	   AND S1.app_id = T1.app_id 
		             			   AND S1.transaction_brand = 'VISA' 
		    WHERE T1.app_customer_code = PARAM_CUSTOMER_CODE
		      AND T1.app_type_file = 'IN'
		      AND T1.app_processing_date = PARAM_PROCESSING_DATE
		      AND T1.local_transaction_date IS NOT NULL
		    LIMIT PARAM_ROW_LIMIT;
		   
		ELSE 
		
		    CREATE TEMPORARY TABLE tmp_table_to_return AS
		    SELECT T1.app_customer_code                            customer_code,
		           T1.app_hash_file                                file_id,
		           T1.app_id::INT                                  row_id,
		           CASE
		               WHEN T1.issuer_acquirer_indicator = 'I' THEN M2.country_code_alternative
		               WHEN T1.issuer_acquirer_indicator = 'A' THEN M1.country_code_alternative
		               END::TEXT                                   customer_country_code,
		           T1.issuer_acquirer_indicator::TEXT              business_mode_code,
		           'VI'                                            brand_code,
		           T1.app_processing_date                          processing_date,
		           LEFT(T1.app_processing_date::TEXT, 7)           processing_month,
		           T1.local_transaction_date                       transaction_date,
		           LEFT(T1.local_transaction_date::TEXT, 7)        transaction_month,
		           CASE
		               WHEN T2.transaction_code_sms IN ('05', '25') THEN 'PUR'
		               WHEN T2.transaction_code_sms IN ('06', '26') THEN 'CRD'
		               WHEN T2.transaction_code_sms IN ('07', '27') THEN 'CSH'
		               ELSE 'OTH'
		               END                                         transaction_group_code,
		           T2.business_transaction_type                    transaction_type_id,
		           T2.jurisdiction::TEXT                           jurisdiction_code,
		           M1.country_code_alternative::TEXT               merchant_country_code,
		           T1.merchants_type                               mcc_code,
		           TRIM(UPPER(T1.card_acceptor_name))::TEXT        merchant,
		           TRIM(UPPER(T1.card_acceptor_id_sms))::TEXT      merchant_id,
		           TRIM(UPPER(T1.card_acceptor_terminal_id))::TEXT terminal_id,
		           T1.acquiring_institution_id_1::INT              acquirer_bin,
		           M2.country_code_alternative::TEXT               issuer_country_code,
		           LEFT(T1.card_number::TEXT, 6)::INT              issuer_bin_6,
		           LEFT(T1.card_number::TEXT, 8)::INT              issuer_bin_8,
		           T2.funding_source::TEXT                         funding_source_code,
		           M5.range_program_id::INT                        product_program_id,
		           T2.product_id                                   product_code,
		           CASE
		               WHEN T1.mailtelephone_or_electronic_commerce_indicator = ' ' THEN 'CPR'
		               WHEN REPLACE(T1.mailtelephone_or_electronic_commerce_indicator, ' ', '0')::INT BETWEEN 1 AND 9
		                   THEN 'CNP'
		               ELSE 'UNK'
		               END                                         card_present_code,
		           T2.reversal_indicator::BOOLEAN                  is_reversal_or_chargeback,
		           M4.fee_descriptor::TEXT                         interchange_rule,
		           M6.report_currency_code::TEXT                   reported_currency_code,
		           CASE
		               WHEN T1.transaction_amount = 0
		                   THEN COALESCE(X3.exchange_value, 1) * (T1.cryptogram_amount + T1.surcharge_amount_sms)
		               ELSE COALESCE(X1.exchange_value, 1) * T1.transaction_amount
		               END                                         transaction_amount,
		           CASE
		               WHEN T1.transaction_amount = 0 AND
		                    (T3.fee_currency IS NULL OR T3.fee_currency <> M6.report_currency_code)
		                   THEN COALESCE(X3.exchange_value, 1) * T3.calculated_value
		               WHEN T1.transaction_amount = 0 AND T3.fee_currency = M6.report_currency_code
		                   THEN COALESCE(X2.exchange_value, 1) * T3.calculated_value
		               ELSE COALESCE(X2.exchange_value, X1.exchange_value, 1) * T3.calculated_value
		               END                                         interchange_fees_amount,
		           S1.unitary_scheme_fee_cost::NUMERIC             scheme_fees_amount
		    FROM operational.dh_visa_transaction_sms T1
		             LEFT JOIN operational.m_country M1 ON M1.country_code = T1.card_acceptor_country
		             LEFT JOIN operational.dh_visa_transaction_sms_calculated_field T2
		                       ON T2.app_customer_code = T1.app_customer_code
		                           AND T2.app_type_file = T1.app_type_file
		                           AND T2.app_processing_date = T1.app_processing_date
		                           AND T2.app_hash_file = T1.app_hash_file
		                           AND T2.app_id = T1.app_id
		             LEFT JOIN operational.m_country M2 ON M2.country_code = T2.ardef_country
		             LEFT JOIN operational.dh_visa_sms_interchange T3
		                       ON T3.app_customer_code = T1.app_customer_code
		                           AND T3.app_type_file = T1.app_type_file
		                           AND T3.app_processing_date = T1.app_processing_date
		                           AND T3.app_hash_file = T1.app_hash_file
		                           AND T3.app_id = T1.app_id
		             LEFT JOIN operational.m_interchange_rules_visa M4
		                       ON M4.region_country_code = T3.region_country_code
		                           AND T3.app_processing_date BETWEEN M4.valid_from AND COALESCE(M4.valid_until, '9999-12-31')
		                           AND M4.intelica_id = T3.intelica_id::TEXT
		             LEFT JOIN operational.m_visa_bin_products M5 ON M5.bin_product_id = T2.product_id
		             LEFT JOIN control.t_customer M6 ON M6.code = T1.app_customer_code
		             LEFT JOIN operational.dh_exchange_rate X1
		                       ON X1.app_processing_date = T1.app_processing_date
		                           AND X1.brand = 'VISA'
		                           AND X1.currency_from_code::INT = T1.transaction_currency_code::INT
		                           AND X1.currency_to = M6.report_currency_code
		             LEFT JOIN operational.dh_exchange_rate X2
		                       ON X2.app_processing_date = T3.app_processing_date
		                           AND X2.brand = 'VISA'
		                           AND X2.currency_from = T3.fee_currency
		                           AND X2.currency_to = M6.report_currency_code
		             LEFT JOIN operational.dh_exchange_rate X3
		                       ON X3.app_processing_date = T1.app_processing_date
		                           AND X3.brand = 'VISA'
		                           AND X3.currency_from_code = '840' -- USD; Special Case.
		                           AND X3.currency_to = M6.report_currency_code
		             LEFT JOIN operational.mh_transaction_scheme_fee S1
		                   	   ON S1.app_customer_code=t1.app_customer_code
			                   	   AND S1.app_type_file = T1.app_type_file
			                   	   AND S1.app_processing_date = T1.app_processing_date
		             			   AND S1.app_hash_file = T1.app_hash_file
			                   	   AND S1.app_id = T1.app_id 
		             			   AND S1.transaction_brand = 'VISA' 
		    WHERE T1.app_customer_code = PARAM_CUSTOMER_CODE
		      AND T1.app_type_file = 'IN'
		      AND T1.app_processing_date = PARAM_PROCESSING_DATE
		      AND T1.local_transaction_date IS NOT null
		      and S1.table_description <> 'VISA ON-US DUP (SMS TO ISS)'
		    LIMIT PARAM_ROW_LIMIT;		
		   
		   	DROP TABLE IF EXISTS tmp_table_dup;
		   
		   	CREATE TEMPORARY TABLE tmp_table_dup AS 
		   	SELECT *
		   	FROM tmp_table_to_return t1
		   	WHERE t1.jurisdiction_code ='on-us'	AND t1.business_mode_code='A';
		   
		   	INSERT INTO tmp_table_to_return
		   	SELECT T1.customer_code,
		   		   T1.file_id,
		   		   T1.row_id,
		   		   T1.customer_country_code,
		   		   'I' business_mode_code,
		   		   T1.brand_code,
		   		   T1.processing_date,
		   		   T1.processing_month,
		   		   T1.transaction_date,
		   		   T1.transaction_month,
		   		   T1.transaction_group_code,
		   		   T1.transaction_type_id,
		   		   T1.jurisdiction_code,
		   		   T1.merchant_country_code,
		   		   T1.mcc_code,
		   		   T1.merchant,        
		   		   T1.merchant_id,    
		   		   T1.terminal_id,
		   		   T1.acquirer_bin,
		   		   T1.issuer_country_code,
		   		   T1.issuer_bin_6,
		   		   T1.issuer_bin_8,
		   		   T1.funding_source_code,
		   		   T1.product_program_id,
		   		   T1.product_code,
		   		   T1.card_present_code,
		   		   T1.is_reversal_or_chargeback,
		   		   T1.interchange_rule,
		   		   T1.reported_currency_code,
		   		   T1.transaction_amount,  
		   		   T1.interchange_fees_amount,
		   		   T2.unitary_scheme_fee_cost::NUMERIC scheme_fees_amount
		   	FROM tmp_table_dup T1
		   		LEFT JOIN operational.mh_transaction_scheme_fee T2 ON T2.app_customer_code= T1.customer_code
		       	   AND T2.app_type_file = 'IN'
		       	   AND T2.app_processing_date = T1.processing_date
				   AND T2.app_hash_file = T1.file_id
		       	   AND T2.app_id = T1.row_id 
				   AND T2.transaction_brand = 'VISA'	
				   AND T2.table_description = 'VISA ON-US DUP (SMS TO ISS)';
		
		END IF;
	
	    UPDATE tmp_table_to_return T1
	    SET transaction_amount      = T1.transaction_amount * -1,
	        interchange_fees_amount = T1.interchange_fees_amount * -1
	    WHERE T1.is_reversal_or_chargeback IS TRUE;
	
	    RETURN QUERY
	        SELECT *
	        FROM tmp_table_to_return;
	END;	       

END;

$function$
;
