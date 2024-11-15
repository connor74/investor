CREATE TABLE portfolio.deals (
	contact varchar(7) NOT NULL,
	deal_num varchar(12) NOT NULL,
	deal_date DATE NOT NULL,
	sec_code varchar(15) NOT NULL,
	sec_type varchar(20) NOT NULL,
	market_type varchar(25) NOT NULL,
	oper_type varchar(12) NOT NULL,
	volume int NOT NULL,
	price decimal(15, 8),
	acc_in decimal(9,4),
	amount decimal(15, 8),
	currency varchar(3),
	CONSTRAINT deals_pkey PRIMARY KEY (deal_num, deal_date)
);