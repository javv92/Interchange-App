--2 Insertar en el visa adapter el nuevo registro
INSERT INTO "control".t_visa_adapter
(tc, tcr, position, length, column_name, start_date, end_date, additional_condition, tcr_sub, type_record, condition_type_record, column_type, column_decimal, app_creation_date, app_creation_user)
VALUES('46', '0', 94, 1, 'amount type 110', '2022-01-01', NULL, 'P61-63 == ''110''&P64-65 == ''  ''', '110', 'vss_110', NULL, 'varchar', 0, now(), 'root');