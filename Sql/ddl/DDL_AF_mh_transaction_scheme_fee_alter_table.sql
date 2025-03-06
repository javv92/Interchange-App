alter table operational.mh_transaction_scheme_fee
add column "card_acceptor_id" text;

alter table operational.mh_transaction_scheme_fee
add column "switch_code" varchar(4);

alter table operational.mh_monthly_scheme_fee
add column "switch_code" varchar(4);

alter table operational.mh_monthly_scheme_fee_legacy
add column "swt_cd" varchar(4);