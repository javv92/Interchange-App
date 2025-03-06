CREATE TABLE data_review.obs_raw_visa_transactions_hm (
    app_id numeric,
    app_type_file text,
    app_customer_code text,
    app_hash_file text,
    transaction_code text,
    app_processing_date date,
    account_number integer,
    message_error text,
    fch_registro timestamp DEFAULT CURRENT_TIMESTAMP NULL
);
CREATE INDEX obs_raw_visa_transactions_date_customer ON ONLY data_review.obs_raw_visa_transactions_hm USING btree (app_customer_code);
CREATE INDEX obs_raw_visa_transactions_date_idx ON ONLY data_review.obs_raw_visa_transactions_hm USING btree (app_processing_date);
CREATE INDEX obs_raw_visa_transactions_idx ON ONLY data_review.obs_raw_visa_transactions_hm USING btree (app_id, app_hash_file);