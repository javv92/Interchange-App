-- 1 agregar la columna
alter table operational.stg_adapter_visa_transaction_vss_110
add column "amount type 110" varchar(1);

alter table operational.dh_visa_transaction_vss_110
add column "amount_type_110" varchar(1);