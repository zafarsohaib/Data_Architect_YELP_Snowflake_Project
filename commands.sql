/* Using SnowSQL for command line
>>snowsql.exe -a my12716.eu-central-1 -u SOHAIB */

/* Create and use database */

CREATE DATABASE UDACITY_PROJECT;
USE DATABASE UDACITY_PROJECT;

/* Create and use STAGING schema */

CREATE SCHEMA STAGING;
USE SCHEMA STAGING;

/* Create JSON file format */

create or replace file format myjsonformat type='JSON' strip_outer_array=true;

/* Create CSV file format */

create or replace file format mycsvformat type='CSV' compression='auto'
field_delimiter=',' record_delimiter = '\n'  skip_header=1 error_on_column_count_mismatch=true null_if = ('NULL', 'null') empty_field_as_null = true;

/* Create JSON staging storage area */

create or replace stage my_json_stage file_format = myjsonformat;

/* Create CSV staging storage area */

create or replace stage my_csv_stage file_format = mycsvformat;

/* Create Staging Tables */
create table business(business_json variant);
create table checkin(checkin_json variant);
create table covid(covid_json variant);
create table review(review_json variant);
create table tip(tip_json variant);
create table user(user_json variant);

create table precipitation(dated STRING,
						   precipitation STRING,
						   precipitation_normal STRING);
						   
create table temperature(dated STRING,
						min_value STRING,
						max_value STRING,
						normal_min STRING,
						normal_max STRING);
						
						
/* Load files from local to staging storage area */

--JSON:
put file:///AT/Trainings/Data_Architect_Udacity/Snowflake_Project/datasets/yelp_data/business.json @my_json_stage auto_compress=true;
put file:///AT/Trainings/Data_Architect_Udacity/Snowflake_Project/datasets/yelp_data/checkin.json @my_json_stage auto_compress=true;
put file:///AT/Trainings/Data_Architect_Udacity/Snowflake_Project/datasets/yelp_data/covid.json @my_json_stage auto_compress=true;
put file:///AT/Trainings/Data_Architect_Udacity/Snowflake_Project/datasets/yelp_data/review.json @my_json_stage auto_compress=true;
put file:///AT/Trainings/Data_Architect_Udacity/Snowflake_Project/datasets/yelp_data/tip.json @my_json_stage auto_compress=true;
put file:///AT/Trainings/Data_Architect_Udacity/Snowflake_Project/datasets/yelp_data/user.json @my_json_stage auto_compress=true;

--CSV:
put file:///AT/Trainings/Data_Architect_Udacity/Snowflake_Project/datasets/climate_data/precipitation.csv  @my_csv_stage auto_compress=true parallel=4;	
put file:///AT/Trainings/Data_Architect_Udacity/Snowflake_Project/datasets/climate_data/temperature.csv  @my_csv_stage auto_compress=true parallel=4;	

/* Copy data from staging area to tables */

--JSON:
copy into business from @my_json_stage/business.json.gz file_format=myjsonformat on_error='skip_file';
copy into checkin from @my_json_stage/checkin.json.gz file_format=myjsonformat on_error='skip_file';
copy into covid from @my_json_stage/covid.json.gz file_format=myjsonformat on_error='skip_file';
copy into review from @my_json_stage/review.json.gz file_format=myjsonformat on_error='skip_file';
copy into tip from @my_json_stage/tip.json.gz file_format=myjsonformat on_error='skip_file';
copy into user from @my_json_stage/user.json.gz file_format=myjsonformat on_error='skip_file';

--CSV:
copy into precipitation from @my_csv_stage/precipitation.csv.gz file_format=mycsvformat on_error='skip_file';
copy into temperature from @my_csv_stage/temperature.csv.gz file_format=mycsvformat on_error='skip_file';

/* Create and use ODS schema */

CREATE SCHEMA ODS;
USE SCHEMA ODS;

/* Create new tables in ODS Schema and insert data from STAGING tables */

-- business:
create table business ( business_id VARCHAR(200),
						name VARCHAR(100),
						address VARCHAR(200),
						city VARCHAR(200),
						state VARCHAR(10),
						postal_code VARCHAR(20),
						latitude FLOAT,
						longitude FLOAT,
						stars INT,
						review_count INT,
						is_open NUMBER,
						attributes OBJECT,
						categories VARCHAR,
						hours VARIANT);
						
						
insert into business 
select business_json:business_id, 
	   business_json:name, 
	   business_json:address,
	   business_json:city, 
	   business_json:state, 
	   business_json:postal_code, 
	   business_json:latitude, 
	   business_json:longitude,
	   business_json:stars, 
	   business_json:review_count,
	   business_json:is_open, 
	   business_json:attributes,
	   business_json:categories, 
	   business_json:hours
	   from UDACITY_PROJECT.STAGING.business;
	   
	   
-- checkin:
create table checkin ( business_id VARCHAR(200),
					   dated VARCHAR);
						
						
insert into checkin (business_id, dated)
select checkin_json:business_id, 
	   checkin_json:"date"
	   from UDACITY_PROJECT.STAGING.checkin;

-- covid:
create table covid ( business_id VARCHAR(200),
					 highlights VARIANT,
					 delivery_or_takeout VARIANT,
					 grubhub_enabled VARIANT,
					 call_to_action_enabled VARIANT,
					 request_quote_enabled VARIANT,
					 covid_banner VARIANT,
					 temporary_closed_until VARIANT,
					 virtual_services_offered VARIANT					 
					 );
					 
insert into covid (business_id, highlights, delivery_or_takeout, grubhub_enabled, call_to_action_enabled, request_quote_enabled, covid_banner, temporary_closed_until, virtual_services_offered)
select covid_json:business_id, 
	   covid_json:highlights,
	   covid_json:"delivery or takeout", 
	   covid_json:"Grubhub enabled",
	   covid_json:"Call To Action enabled", 
	   covid_json:"Request a Quote Enabled",
	   covid_json:"Covid Banner", 
	   covid_json:"Temporary Closed Until",
	   covid_json:"Virtual Services Offered"
	   from UDACITY_PROJECT.STAGING.covid;				 

-- review:
create table review ( review_id VARCHAR(200),
					  user_id VARCHAR(200),
					  business_id VARCHAR(200),
					  stars FLOAT,
					  useful NUMBER,
					  funny NUMBER,
					  cool NUMBER,
					  text VARCHAR,
					  dated STRING				 
					 );

insert into review
select review_json:review_id, 
	   review_json:user_id,
	   review_json:business_id, 
	   review_json:stars,
	   review_json:useful, 
	   review_json:funny,
	   review_json:cool, 
	   review_json:text,
	   review_json:date
	   from UDACITY_PROJECT.STAGING.review;	


-- tip:
create table tip ( user_id VARCHAR(200),
				   business_id VARCHAR(200),
				   text VARCHAR,
				   dated STRING,
				   compliment_count NUMBER
				);
					 
insert into tip
select tip_json:user_id, 
	   tip_json:business_id,
	   tip_json:text, 
	   tip_json:date,
	   tip_json:compliment_count
	   from UDACITY_PROJECT.STAGING.tip;	

-- user
create table user ( user_id VARCHAR(200),
				      name VARCHAR(200),
					  review_count NUMBER,
					  yelping_since STRING,
					  useful NUMBER,
					  funny NUMBER,
					  cool NUMBER,
				      elite VARCHAR,
					  friends VARCHAR,
					  fans NUMBER,
					  average_stars FLOAT,
					  compliment_hot NUMBER,
					  compliment_more NUMBER,
					  compliment_profile NUMBER,
					  compliment_cute NUMBER,
					  compliment_list NUMBER,
					  compliment_note NUMBER,
					  compliment_plain NUMBER,
					  compliment_cool NUMBER,
					  compliment_funny NUMBER,
					  compliment_writer NUMBER,
					  compliment_photos NUMBER
					);
					 
insert into user
select user_json:user_id, 
	   user_json:name,
	   user_json:review_count, 
	   user_json:yelping_since,
	   user_json:useful, 
	   user_json:funny,
	   user_json:cool, 
	   user_json:elite,
	   user_json:friends,
	   user_json:fans, 
	   user_json:average_stars,
	   user_json:compliment_hot, 
	   user_json:compliment_more,
	   user_json:compliment_profile,
	   user_json:compliment_cute, 
	   user_json:compliment_list,
	   user_json:compliment_note, 
	   user_json:compliment_plain,
	   user_json:compliment_cool,
	   user_json:compliment_funny, 
	   user_json:compliment_writer,
	   user_json:compliment_photos
	   from UDACITY_PROJECT.STAGING.user;


-- precipitation

create table precipitation(dated DATE,
						   precipitation FLOAT,
						   precipitation_normal FLOAT);

insert into precipitation
select TO_DATE(dated,'YYYYMMDD'), 
	   CASE WHEN precipitation='T' THEN -1 ELSE CAST(precipitation AS FLOAT) END, 
	   CAST(precipitation_normal AS FLOAT)
	   from UDACITY_PROJECT.STAGING.precipitation;
						   
-- temperature					   
create table temperature(dated DATE,
						min_value FLOAT,
						max_value FLOAT,
						normal_min FLOAT,
						normal_max FLOAT);

insert into temperature
select TO_DATE(dated,'YYYYMMDD'), 
	   CAST(min_value AS FLOAT), 
	   CAST(max_value AS FLOAT), 
	   CAST(normal_min AS FLOAT), 
	   CAST(normal_max AS FLOAT)
	   from UDACITY_PROJECT.STAGING.temperature;

ALTER TABLE BUSINESS ADD PRIMARY KEY (business_id);
ALTER TABLE USER ADD PRIMARY KEY (user_id);
ALTER TABLE REVIEW ADD PRIMARY KEY (review_id);
ALTER TABLE TEMPERATURE ADD PRIMARY KEY (dated);
ALTER TABLE PRECIPITATION ADD PRIMARY KEY (dated);

ALTER TABLE TIP ADD CONSTRAINT FK_ods_tip_1 FOREIGN KEY (business_id) REFERENCES business(business_id);
ALTER TABLE TIP ADD CONSTRAINT FK_ods_tip_2 FOREIGN KEY (user_id) REFERENCES user(user_id);
ALTER TABLE REVIEW ADD CONSTRAINT FK_ods_review_1 FOREIGN KEY (business_id) REFERENCES business(business_id);
ALTER TABLE REVIEW ADD CONSTRAINT FK_ods_review_2 FOREIGN KEY (user_id) REFERENCES user(user_id);
ALTER TABLE CHECKIN ADD CONSTRAINT FK_ods_checkin FOREIGN KEY (business_id) REFERENCES business(business_id);
ALTER TABLE COVID ADD CONSTRAINT FK_covid FOREIGN KEY (business_id) REFERENCES business(business_id);

/* Create and use DATAWAREHOUSE schema */

CREATE SCHEMA DATAWAREHOUSE;
USE SCHEMA DATAWAREHOUSE;

/* SQL queries to integrate climate and yelp data */

select * from business b
join review r on r.business_id = b.business_id
join tip on tip.business_id = b.business_id
join precipitation p on p.dated=r.dated
join temperature temp on temp.dated=r.dated
left join user u on u.user_id=r.user_id
left join checkin ch on ch.business_id = b.business_id
left join covid co on co.business_id = b.business_id;

/* SQL queries to create Dimension Tables */	

CREATE TABLE business CLONE UDACITY_PROJECT.ODS.business;
CREATE TABLE checkin CLONE UDACITY_PROJECT.ODS.checkin;
CREATE TABLE covid CLONE UDACITY_PROJECT.ODS.covid;
CREATE TABLE review CLONE UDACITY_PROJECT.ODS.review;
CREATE TABLE tip CLONE UDACITY_PROJECT.ODS.tip;
CREATE TABLE user CLONE UDACITY_PROJECT.ODS.user;
CREATE TABLE temperature CLONE UDACITY_PROJECT.ODS.temperature;
CREATE TABLE precipitation CLONE UDACITY_PROJECT.ODS.precipitation;

/* SQL queries to create Fact Table */	

create table FactTable
AS
select distinct b.business_id, r.review_id, u.user_id, temp.dated as date_t, p.dated as date_p  from business b
join review r on r.business_id = b.business_id
join tip on tip.business_id = b.business_id
join precipitation p on p.dated=r.dated
join temperature temp on temp.dated=r.dated
left join user u on u.user_id=r.user_id
left join checkin ch on ch.business_id = b.business_id
left join covid co on co.business_id = b.business_id

ALTER TABLE BUSINESS ADD PRIMARY KEY (business_id);
ALTER TABLE USER ADD PRIMARY KEY (user_id);
ALTER TABLE REVIEW ADD PRIMARY KEY (review_id);
ALTER TABLE TEMPERATURE ADD PRIMARY KEY (dated);
ALTER TABLE PRECIPITATION ADD PRIMARY KEY (dated);

ALTER TABLE FACTTABLE ADD CONSTRAINT FK_business FOREIGN KEY (business_id) REFERENCES business(business_id);
ALTER TABLE FACTTABLE ADD CONSTRAINT FK_user FOREIGN KEY (user_id) REFERENCES user(user_id);
ALTER TABLE FACTTABLE ADD CONSTRAINT FK_review FOREIGN KEY (review_id) REFERENCES review(review_id);
ALTER TABLE FACTTABLE ADD CONSTRAINT FK_date_t FOREIGN KEY (date_t) REFERENCES temperature(dated);
ALTER TABLE FACTTABLE ADD CONSTRAINT FK_date_p FOREIGN KEY (date_p) REFERENCES precipitation(dated);

/* SQL queries code that reports the business name, temperature, precipitation, and ratings */

select b.name, t.min_value, t.max_value, t.normal_min, t.normal_max, p.precipitation, p.precipitation_normal, r.stars  
from FACTTABLE f
join business b on b.business_id=f.business_id
join review r on r.review_id=f.review_id
join temperature t on t.dated=f.date_t
join precipitation p on p.dated=f.date_p;

