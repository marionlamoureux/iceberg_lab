# Iceberg Lab

## Summary
This workshop will take you through the new capabilities that have been added to CDP Public Cloud Lakehouse.
In this workshop you will learn how to take advantage of Iceberg to support Data Lakehouse initiatives.
These instructions include:   


**Value Propositions**: Take advantage of Iceberg - CDP’s Open Data Lakehouse, to experience:  
- Better performance  
- Lower maintenance responsibilities  
- Multi-function analytics without having many copies of data  
- Greater control  

*Note to admins: Refer to the Setup file containing the recommendations to setup the lab*


## TABLE OF CONTENT
[1. Introduction to the workshop](#1-introduction-to-the-workshop)  
[2. Iceberg with NiFi and Sql Stream Builder](2-introduction-to-iceberg-with-nifi)  
[3. Introduction to Iceberg with Sql Stream Builder](2-introduction-to-iceberg-with-nifi)


### 1. Introduction-to-the-workshop  
Data set for this workshop is the publicly available Airlines data set, which consists of c.80million row of flight information across the United States.  
Schema for the data set is below:Entity-Relation Diagram of tables we use in todays workshop:

Fact table: flights (86mio rows)
Dimension tables: airlines (1.5k rows), airports (3.3k rows) and planes.

**Dataset airlines schema**  

![Airlines schema](./images/Iceberg_airlinesschema.png)

**Raw format of the flight set**  
Here displayed in a file explorer:

![Raw Data Set](./images/dataset_folder.png)


Make a note of your username, in a CDP Public Cloud workshop, its should be a account from user01 to user50, 
assigned by your workshop presenter.
Check that the airlines data was ingested for you: you should have a database named after your username.


Navigate to Data Warehouse, then Virtual Warehouse and open the SQL Authoring tool HUE.
![Home_CDW](./images/home_cdw.png)

Execute the following in HUE Impala Editor to test that data has loaded correctly and 
that you have the appropriate access.
![Hue Editor](./images/Hue_editor.png)


```SQL
SELECT COUNT(*) FROM ${username}_airlines_csv.flights_csv;
```

![Flights data](./images/Iceberg_Flightsdata.png)

```SQL
-- CREATE DATABASES
-- EACH USER RUNS TO CREATE DATABASES
CREATE DATABASE ${username}_airlines;
CREATE DATABASE ${username}_airlines_maint;


-- CREATE HIVE TABLE FORMAT TO CONVERT TO ICEBERG LATER
drop table if exists ${user_id}_airlines.planes;

CREATE TABLE ${user_id}_airlines.planes (
  tailnum STRING, owner_type STRING, manufacturer STRING, issue_date STRING,
  model STRING, status STRING, aircraft_type STRING,  engine_type STRING, year INT 
) 
TBLPROPERTIES ( 'transactional'='false' )
;

INSERT INTO ${user_id}_airlines.planes
  SELECT * FROM airlines_csv.planes_csv;

-- HIVE TABLE FORMAT TO USE CTAS TO CONVERT TO ICEBERG
drop table if exists ${user_id}_airlines.airports_hive;

CREATE TABLE ${user_id}_airlines.airports_hive
   AS SELECT * FROM airlines_csv.airports_csv;

-- HIVE TABLE FORMAT
drop table if exists ${user_id}_airlines.unique_tickets;

CREATE TABLE ${user_id}_airlines.unique_tickets (
  ticketnumber BIGINT, leg1flightnum BIGINT, leg1uniquecarrier STRING,
  leg1origin STRING,   leg1dest STRING, leg1month BIGINT,
  leg1dayofmonth BIGINT, leg1dayofweek BIGINT, leg1deptime BIGINT,
  leg1arrtime BIGINT, leg2flightnum BIGINT, leg2uniquecarrier STRING,
  leg2origin STRING, leg2dest STRING, leg2month BIGINT, leg2dayofmonth BIGINT,
  leg2dayofweek BIGINT, leg2deptime BIGINT, leg2arrtime BIGINT 
);

INSERT INTO ${user_id}_airlines.unique_tickets
  SELECT * FROM airlines_csv.unique_tickets_csv;

-- CREATE ICEBERG TABLE FORMAT STORED AS PARQUET
drop table if exists ${user_id}_airlines.planes_iceberg;

CREATE TABLE ${user_id}_airlines.planes_iceberg
   STORED AS ICEBERG AS
   SELECT * FROM airlines_csv.planes_csv;

-- CREATE ICEBERG TABLE FORMAT STORED AS PARQUET
drop table if exists ${user_id}_airlines.flights_iceberg;

CREATE TABLE ${user_id}_airlines.flights_iceberg (
 month int, dayofmonth int, 
 dayofweek int, deptime int, crsdeptime int, arrtime int, 
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string, 
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, 
 depdelay int, origin string, dest string, distance int, taxiin int, 
 taxiout int, cancelled int, cancellationcode string, diverted string, 
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, 
 lateaircraftdelay int
) 
PARTITIONED BY (year int)
STORED AS ICEBERG 
;

-- LOAD DATA INTO ICEBERG TABLE FORMAT STORED AS PARQUET
INSERT INTO ${user_id}_airlines.flights_iceberg
 SELECT * FROM airlines_csv.flights_csv
 WHERE year <= 2006;

--
-- TABLES NEEDED FOR THE NIFI LAB
DROP TABLE IF EXISTS ${user_id}_airlines.`routes_nifi_iceberg`;
CREATE TABLE ${user_id}_airlines.`routes_nifi_iceberg` (
  `airline_iata` VARCHAR,
  `airline_icao` VARCHAR,
  `departure_airport_iata` VARCHAR,
  `departure_airport_icao` VARCHAR,
  `arrival_airport_iata` VARCHAR,
  `arrival_airport_icao` VARCHAR,
  `codeshare` BOOLEAN,
  `transfers` BIGINT,
  `planes` ARRAY<VARCHAR>
) STORED AS ICEBERG;

DROP TABLE IF EXISTS ${user_id}_airlines.`airports_nifi_iceberg`;
CREATE TABLE ${user_id}_airlines.`airports_nifi_iceberg` (
  `city_code` VARCHAR,
  `country_code` VARCHAR,
  `name_translations` STRUCT<`en`:string>,
  `time_zone` VARCHAR,
  `flightable` BOOLEAN,
  `coordinates` struct<`lat`:DOUBLE, `lon`:DOUBLE>,
  `name` VARCHAR,
  `code` VARCHAR,
  `iata_type` VARCHAR
) STORED AS ICEBERG;

DROP TABLE IF EXISTS ${user_id}_airlines.`countries_nifi_iceberg`;
CREATE TABLE ${user_id}_airlines.`countries_nifi_iceberg` (
  `name_translations` STRUCT<`en`:VARCHAR>,
  `cases` STRUCT<`su`:VARCHAR>,
  `code` VARCHAR,
  `name` VARCHAR,
  `currency` VARCHAR
) STORED AS ICEBERG;

-- [TABLE MAINTENANCE] CREATE FLIGHTS TABLE IN ICEBERG TABLE FORMAT
drop table if exists ${user_id}_airlines_maint.flights;

CREATE TABLE ${user_id}_airlines_maint.flights (
 month int, dayofmonth int, 
 dayofweek int, deptime int, crsdeptime int, arrtime int, 
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string, 
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, 
 depdelay int, origin string, dest string, distance int, taxiin int, 
 taxiout int, cancelled int, cancellationcode string, diverted string, 
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, 
 lateaircraftdelay int
) 
PARTITIONED BY (year int)
STORED AS ICEBERG 
;
```

```SQL
-- LOAD DATA TO SIMULATE SMALL FILES
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 1;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 2;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 3;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 4;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 5;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 6;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 7;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 8;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 9;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 10;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 11;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 12;

INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 1;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 2;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 3;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 4;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 5;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 6;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 7;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 8;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 9;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 10;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 11;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 12;
```


Copy & paste the SQL below into HUE,
```SQL
-- TEST PLANES PROPERTIES
DESCRIBE FORMATTED ${user_id}_airlines.planes;
```

Pay attention to the following properties: 
- Table Type: `EXTERNAL`
- SerDe Library: `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe`
- Location: `warehouse/tablespace/external/hive/`

The following labs will take you through various CDP Public Cloud to enable you on what will be available to support Data Lakehouse use cases. 
CDP Public Cloud now includes support for Apache Iceberg in the following services: Impala, Flink, SSB, Spark 3, NiFi, and Replication (BDR). 
This makes Cloudera the only vendor to support Iceberg in a multi-hybrid cloud environment. 
Users can develop an Iceberg application once and deploy anywhere.  

**Handy Iceberg Links**  
[Apache Iceberg Documentation (be careful not everything may be supported yet in CDP)](https://iceberg.apache.org/docs/latest/)  
[Impala Iceberg Cheatsheet](https://docs.google.com/document/d/1cusHyLBA7hS5zLV0vVctymoEbUviJi4aT8SfKyIe_Ao/edit?usp=drive_link)  

### 2. Introduction to Iceberg with NiFi  

NiFi/Flink/SSB (Ingest Data ,Stream Data, SQL Stream Builder Queries)
In this lab we are going to use Nifi, Flink, and Sql Stream Builder to complete sample integrations with Iceberg.
First, we will use NiFi to ingest an airport route data set (JSON) and send that data to Kafka and Iceberg. 
Next we will use NiFi to ingest a countries data set (JSON) and send to Kafka and Iceberg.  
Finally we will use NiFi to ingest an airports data set (JSON) and send to Kafka and Iceberg.  

**Execute the following in NiFi**  

Let's deploy our NiFi flows. Download flow definition file here:  

https://github.com/cldr-steven-matison/NiFi-Templates/blob/main/SSBDemo.json

Save it as a JSON file locally and access CDF to deploy it:

### 3. Introduction to Iceberg with Sql Stream Builder  
Once we are complete with NiFi, we will shift into Sql Streams Builder to show its capability to query Kafka with SQL,
Infer Schema, Create Iceberg Connectors,  and use SQL to INSERT INTO an Iceberg Table.  
Finally we will wrap up by jumping back into Hue and taking a look at the tables we created.


**Download you Kerberos Keytab**  
On the left hand menu, click on your username and access the Profile menu. On the right, under Actions, click Get keytab.  
![Get Keytab](./images/Iceberg_GetKeytab.png)  


**Copy/Paste the Kafka Endpoints**  
In CDP Public Cloud, Kafka is deployed in a Datahub, which is a step previously setup by the lab admin, 
The Endpoints are available on the overview page of the Datahub indicated by the admin, on the bottom menu,
under "endpoints".  
Kafka Endpoints in Datahub overview
![Kafka Borker Endpoints](./images/Iceberg_KafkaBorkerEndpoints.png)

**Copy/paste the thrift Hive URI**  
In you virtual warehouse, copy the JDBC url and keep only the node name in the string:  
`hs2-asdf.dw-go01-demo-aws.ylcu-atmi.cloudera.site`

**Download the configuration files**
In your environment, access the Cloudera manager page under "data lake"
![Configurationfiles](./images/Iceberg_downloadclientconfiguration.png)

**Access CDF**  
Access the CDF Catalog and deploy flow  in the environment indicated by your admin.


|Parameter|Value|
|----------|----------|
|CDP Workload User|<enter your user id>|
|CDP Workload User Password|<enter your login password> and set sensitive to ‘Yes’|
|Hadoop Configuration Resources|/etc/hive/conf/hive-site.xml,/etc/hadoop/conf/core-site.xml,/etc/hive/conf/hdfs-site.xml|
|Kerberos Keytab|/tmp/<user-id>_nifi.keytab|
|Hive Metastore URI|thrift://base1-01.lab##.pvc-ds-bc.athens.cloudera.com:9083,thrift://base2-01.lab##.pvc-ds-bc.athens.cloudera.com:9083|
|Kafka Broker Endpoint|`Kafka Endpoints`|  

Access SSB and perform the below steps:

1. Import this repo as a project in Sql Stream Builder
2. Open your Project and have a look around at the left menu. Notice all hover tags. Explore the vast detail in Explorer menus.
3. Import Your Keytab
4. Check out Source Control. If you created vs import on first screen you may have to press import here still. You can setup Authentication here.
5. Create and activate an Environment with a key value pair for your userid -> username
6. Inspect/Add Data Sources. You may need to re-add Kafka. The Hive data source should work out of the box.
7. Inspect/Add Virtual Kafka Tables. You can edit the existing tables against your kafka data source and correct topics. Just be sure to choose right topics and detect schema before save.

## Modifications to Jobs
Note: current repo should not require any job modifications.

**CSA_1_11_Iceberg_Sample** - Example in CSA 1.11 docs  
No modifications should be required to this job  
**Countries_Kafka** - Select from Kafka Countries, Create IceBerg Table, Insert Results  
Confirm Kafka topic  
**Routes_Kafka** - Select from Kafka Routes, Create IceBerg Table, Insert Results  
Confirm Kafka topic  
**Test_Hue_Tables**  
Confirm source iceberg table exists, check table names, and namespaces.  
**Time_Travel**  
Execute required DESCRIBE in Hue, use SnapShot Ids  


### Top Tips
If you are using different topics w/ different schema, use SSB to get the DDL for topic. Copy paste into the ssb job's create statement and begin modifying to acceptance. Just be careful with complicated schema such as array, struct, etc.
If you are testing CREATE and INSERT in iterations, you should increment all table names per test iteration. Your previous interations will effect next iterations so stay in unique table names.
Use DROP statement with care. It will DROP your Virtual Table, but not necessarily the impala/hive table. DROP those in HUE if needed.
Execution of Jobs:
Warning: These are not full ssb jobs. In these are samples you execute each statements one at a time.

```
Execute the SELECT * FROM kafka_topic. This will confirm that you have results in your kafka topic. Be patient, if this is your first job may take some time (1-2 minutes) to report results.
Execute the CREATE TABLE Statement. This will create the virtual table in ssb_default name space. It will not create the table in IMPALA.
Execute the INSERT INTO SELECT. Be Patient. This will create the impala table and begin reporting results from the kafka topic shortly.
Lastly, execute the final select. These results are from IMPALA.
```

