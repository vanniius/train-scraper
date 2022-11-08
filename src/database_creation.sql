create TABLE date (
	date_key int PRIMARY KEY,
	calendar_date date,
	YEAR int,
	MONTH varchar,
	month_num int,
	week int,
	weekday varchar,
	weekday_num int
	);

CREATE TABLE stations (
	station_id int PRIMARY KEY,
	station_name varchar
	);

CREATE TABLE train_data (
	id_key varchar PRIMARY KEY,
-- composed key: date + tech_id + com_id
id_date int REFERENCES date (date_key),
	tech_id int,
	com_id int,
	line varchar,
	actual_origin_code int REFERENCES stations (station_id),
	actual_destination_code int REFERENCES stations (station_id),
	actual_origin_departure_time timestamp,
	actual_destination_arrival_time timestamp,
	last_stop_code int REFERENCES stations (station_id),
	last_stop_delay int,
	is_pmr boolean,
	is_disabled boolean,
	is_cancelled boolean,
	data_timestamp timestamp
	);

CREATE TABLE schedule (
	train_id varchar REFERENCES train_data (id_key),
	stop_code int REFERENCES stations (station_id),
	stop_arrival_time timestamp,
	stop_departure_time timestamp,
	stop_order int,
	schedule_timestamp timestamp
	);

CREATE TABLE tracking (
	train_id varchar REFERENCES train_data(id_key),
	next_stop_code int REFERENCES stations (station_id),
	current_delay int,
	obtained_at timestamp
	);
