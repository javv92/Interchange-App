CREATE TABLE operational.m_bin_ranges (
	start_value numeric NOT NULL,
	end_value numeric NULL,
    customer_code varchar(6) NOT NULL,
	country_code varchar(5) NOT NULL,
    switch_code varchar(4) NOT NULL,
	brand text NOT NULL
);
