-- CREATE TABLE users (
-- 	user_id serial not null,
-- 	signup_date DATE not null,
-- 	country character varying(50),
-- 	acqusition_channel character varying(100) check (acqusition_channel in ('organic', 'referral', 'google_ads', 'instagram_ads', 'partnerships', 'tiktok_ads')),
-- 	constraint pkey_userid primary key (user_id)
-- );

-- CREATE TABLE events (
-- 	event_id serial not null,
-- 	user_id integer not null,
-- 	event_name character varying(255),
-- 	event_timestamp timestamp,
-- 	device_type character varying(255),
-- 	platform character varying(50) check (platform in ('android', 'ios', 'web')),
-- 	constraint pkey_eventid primary key (event_id),
-- 	constraint fkkey_userid foreign key (user_id) references users (user_id)
-- );


-- CREATE TABLE transactions (
-- 	txn_id serial not null,
-- 	user_id integer not null,
-- 	amount numeric(10,2),
-- 	currency character varying(55),
-- 	fee numeric(10,2),
-- 	device_type character varying(255),
-- 	txn_type character varying(50) check (txn_type in ('deposit', 'withdrawal', 'transfer', 'merchant_payment')),
-- 	constraint pkey_txnid primary key (txn_id),
-- 	constraint afkkey_userid foreign key (user_id) references users (user_id)
-- );

CREATE TABLE marketing_spend (
	date date not null,
	channel character varying(255) not null,
	spend numeric(10,2)
);



