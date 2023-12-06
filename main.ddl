-------------------------------------------------------------------------
--main.DDL
-------------------------------------------------------------------------
-----------------------------TERMINALS-----------------------------------
-- deaian.lapp_stg_terminals

CREATE TABLE deaian.lapp_stg_terminals (
	terminal_id VARCHAR(30)
 	,terminal_type VARCHAR(50)
 	,terminal_city VARCHAR(50)
 	,terminal_address VARCHAR(50)
 	,create_dt timestamp(0)
	,update_dt timestamp(0)
	,processed_dt timestamp(0)
 );


-- deaian.lapp_stg_terminals_del

CREATE TABLE deaian.lapp_stg_terminals_del (
	terminal_id VARCHAR(30)
);


-- deaian.lapp_dwh_dim_terminals SCD1

CREATE TABLE deaian.lapp_dwh_dim_terminals (
	terminal_id VARCHAR(30)
 	,terminal_type VARCHAR(50)
 	,terminal_city VARCHAR(50)
 	,terminal_address VARCHAR(50)
	,create_dt timestamp(0)
	,update_dt timestamp(0)
	,processed_dt timestamp(0)
 );


-----------------------------CLIENTS---------------------------------------

-- deaian.lapp_stg_clients

CREATE TABLE deaian.lapp_stg_clients (
	client_id VARCHAR(10)
	,last_name VARCHAR(20)
	,first_name VARCHAR(20)
	,patronymic VARCHAR(20)
	,date_of_birth DATE
	,passport_num VARCHAR(15)
	,passport_valid_to DATE
	,phone BPCHAR(16)
	,create_dt timestamp(0)
	,update_dt timestamp(0)
	,processed_dt timestamp(0)
);


--deaian.lapp_stg_clients_del

CREATE TABLE deaian.lapp_stg_clients_del (
	client_id VARCHAR(10)
);


-- deaian.lapp_dwh_dim_clients SCD1

CREATE TABLE deaian.lapp_dwh_dim_clients (
	client_id VARCHAR(10)
	,last_name VARCHAR(20)
	,first_name VARCHAR(20)
	,patronymic VARCHAR(20)
	,date_of_birth DATE
	,passport_num VARCHAR(15)
	,passport_valid_to DATE
	,phone BPCHAR(16)
	,create_dt timestamp(0)
	,update_dt timestamp(0)
	,processed_dt timestamp(0)
);


----------------------------------ACCOUNTS------------------------------------

-- deaian.lapp_stg_accounts

CREATE TABLE deaian.lapp_stg_accounts (
	account_num BPCHAR(20)
	,valid_to DATE
	,client VARCHAR(10)
	,create_dt timestamp(0)
	,update_dt timestamp(0)
	,processed_dt timestamp(0)
);


--deaian.lapp_stg_accounts_del

CREATE TABLE deaian.lapp_stg_accounts_del (
	account_num BPCHAR(20)
);
	

-- deaian.lapp_dwh_dim_accounts SCD1

CREATE TABLE deaian.lapp_dwh_dim_accounts (
	account_num BPCHAR(20)
	,valid_to DATE
	,client VARCHAR(10)
	,create_dt timestamp(0)
	,update_dt timestamp(0)
	,processed_dt timestamp(0)
);


----------------------------------CARDS----------------------------------------

-- deaian.lapp_stg_cards

CREATE TABLE deaian.lapp_stg_cards (
	card_num VARCHAR(30)
	,account_num BPCHAR(20)
	,create_dt timestamp(0)
	,update_dt timestamp(0)
	,processed_dt timestamp(0)
);


--deaian.lapp_stg_cards_del

CREATE TABLE deaian.lapp_stg_cards_del (
	card_num VARCHAR(30)
);


-- deaian.lapp_dwh_dim_cards SCD1

CREATE TABLE deaian.lapp_dwh_dim_cards (
	card_num VARCHAR(30)
	,account_num BPCHAR(20)
	,create_dt timestamp(0)
	,update_dt timestamp(0)
	,processed_dt timestamp(0)
);


------------------------------TRANSACTIONS-------------------------------------

-- deaian.lapp_stg_transactions

CREATE TABLE deaian.lapp_stg_transactions (
 	trans_id VARCHAR(30)
 	,trans_date TIMESTAMP(0)
 	,amt DECIMAL(18,5)
 	,card_num VARCHAR(30)
 	,oper_type VARCHAR(50)
	,oper_result VARCHAR(50)
	,terminal VARCHAR(50)
);


-- deaian.lapp_dwh_fact_transactions

CREATE TABLE deaian.lapp_dwh_fact_transactions (
 	trans_id VARCHAR(30)
 	,trans_date TIMESTAMP(0)
 	,amt DECIMAL(18,5)
 	,card_num VARCHAR(30)
 	,oper_type VARCHAR(50)
	,oper_result VARCHAR(50)
	,terminal VARCHAR(50)
);


---------------------------------BLACKLIST----------------------------------------

-- deaian.lapp_stg_blacklist

CREATE TABLE deaian.lapp_stg_blacklist (
	entry_dt TIMESTAMP(0)
	,passport_num VARCHAR(15)
);


-- deaian.lapp_dwh_fact_passport_blacklist

CREATE TABLE deaian.lapp_dwh_fact_passport_blacklist (
	entry_dt TIMESTAMP(0)
	,passport_num VARCHAR(15)
);


-- Дополнительные таблицы
---------------------------------REP_FRAUD---------------------------------------------

-- deaian.lapp_rep_fraud

CREATE TABLE deaian.lapp_rep_fraud ( 
	event_dt TIMESTAMP(0)
	,passport VARCHAR(15)
	,fio VARCHAR(60)
	,phone BPCHAR(16)
	,event_type VARCHAR(50)
	,report_dt TIMESTAMP(0)
);


-------------------------------------META----------------------------------------------

CREATE TABLE deaian.lapp_meta (
    schema_name varchar(30),
    table_name varchar(50),
    max_update_dt timestamp(0)
);

