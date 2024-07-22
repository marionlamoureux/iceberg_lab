# Iceberg and SQL Stream Builder Lab

## Summary
This workshop will take you through the new capabilities that have been added to CDP Public Cloud Lakehouse and into the various features of the Sql stream builder.

In this workshop you will learn how to take advantage of Iceberg to support Data Lakehouse initiatives.

**Value Propositions**:  

Take advantage of Iceberg - **CDP Open Data Lakehouse**, to experience:  
- Better performance  
- Lower maintenance responsibilities  
- Multi-function analytics without having many copies of data  
- Greater control  

*Note to admins: Refer to the Setup file containing the recommendations to setup the lab*


## TABLE OF CONTENT
  * [1. Introduction to the workshop](#1-introduction-to-the-workshop)  
    * [1.1. Logging in](##11-logging-in)  
    * [1.2. Data Set](##11-data-set)   
    * [1.3. Access the data set in Cloudera Data Warehouse](##13-access-the-data-set-in-cloudera-data-warehouse)  
    * [1.4. Generating the Iceberg tables](##14-generating-the-iceberg-tables)   
    * [1.5. Loading data](##15-loading-data)  
  * [2. Features of Iceberg](#2-Featured-of-Iceberg) 
    * [2.1. Partition evolution](22-schema-evolution)     
    * [2.2. Partition evolution](22-partition-evolution)  
    * [2.3. Snapshots](##23-snapshots)  

### 1. Introduction to the workshop  
**Goal of the section**: Check the dataset made available in a database in a csv format and store it all as Iceberg.  

#### 1.1. Logging in

Access the url indicated by the presenter and indicate your credentials.

_Make a note of your username, in a CDP Public Cloud workshop, it should be a account from user01 to user50, assigned by your workshop presenter._

#### 1.2. Data Set description

Data set for this workshop is the publicly available Airlines data set, which consists of c.80million row of flight information across the United States.  
For additional information : [Data Set Description](./documentation/IcebergLab-Documentation.md#data-set)

#### 1.3. Access the data set in Cloudera Data Warehouse
In this section, we will check that the airlines data was ingested for you: you should be able to query the master database: `airlines_csv`.   

Each participant will then create their own Hive/Iceberg databases out of the shared master database.

Navigate to Data Warehouse service:
  
![Home_CDW](./images/home_cdw.png)

Then choose an **Impala** Virtual Warehouse and open the SQL Authoring tool HUE. There are two types of virtual warehouses you can create in CDW,
here we'll be using the type that leverages **Impala** as an engine:  

![Typesofvirtualwarehouses.png](./images/ImpalaVW.png)  


Execute the following in **HUE** Impala Editor to test that data has loaded correctly in the mastwr database in a csv format and that you have the appropriate access.   
  
```SQL
SELECT COUNT(*) FROM airlines_csv.flights_csv; 
```

**Expected output**  
![Hue Editor](./images/Hue_editor.png)

  
#### 1.4. Generating the Iceberg tables

In this section, we will generate the Iceberg database from the pre-ingested csv tables.   

**Run the below queries to create your own databases to then ingest data from the master database,**  
  

```SQL
-- CREATE DATABASES
-- EACH USER RUNS TO CREATE DATABASES
CREATE DATABASE ${user_id}_airlines;
CREATE DATABASE ${user_id}_airlines_maint;
```

*Note: These queries use variables in Hue

To set the variable value with your username, fill in the field as below:  

![Setqueryvaribale](./images/Set_variable_hue.png)  


To run several queries in a row in Hue, make sure you select all the queries:*  

![Hue_runqueries.png](./images/Hue_runqueries.png)  


  
**Once the database is created, create the Hive tables first and insert the data with a CTAS command.**  


```SQL
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
  
```
Copy & paste the SQL below into HUE

```SQL
-- TEST PLANES PROPERTIES
DESCRIBE FORMATTED ${user_id}_airlines.planes;
```  

Pay attention to the following properties of your Hive table: 
- Table Type: `EXTERNAL`
- SerDe Library: `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe`
- Location: `warehouse/tablespace/external/hive/`


Create Table as Select (CTAS) - create new table Stored as Icerberg, using the default file format, Parquet.  


```SQL
-- CREATE ICEBERG TABLE FORMAT STORED AS PARQUET
drop table if exists ${user_id}_airlines.planes_iceberg;

CREATE TABLE ${user_id}_airlines.planes_iceberg
   STORED AS ICEBERG AS
   SELECT * FROM airlines_csv.planes_csv;
   
```

**VALUE**: combine Iceberg with Hive table formats, means you can convert to use Iceberg where it makes sense
and also can migrate over time vs. having to migrate at the same time.

```SQL
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
```

Copy & paste the SQL below into HUE

```SQL
-- TEST PLANES PROPERTIES
DESCRIBE FORMATTED ${user_id}_airlines.planes_iceberg;
```

Pay attention to the following properties for this Iceberg table, compared to the same table in Hive format:
- Table Type: `	EXTERNAL_TABLE`
- SerDe Library: `org.apache.iceberg.mr.hive.HiveIcebergSerDe`
- Location: `/warehouse/tablespace/external/hive/user020_airlines.db/planes_iceberg	`


You now have your own database you can run the below queries over:  

![AirlinesDB](./images/AirlinesDB.png)


In this section, we will load data in Iceberg format and demonstrate a few key maintenance features of Iceberg.

#### 2.1. Loading data

Under the 'maintenance' database, let's load the flight table partitioned by year.  


```SQL
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

**Partition evolution**: the insert queries below are designed to demonstrate partition evolution
and snapshot feature for time travel 


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

### 2. Features of Iceberg

#### 2.1. Schema evolution



#### 2.2. Partition evolution  

**Partition Evolution Using SQL**
This module explores partition evolution for Iceberg tables on the Cloudera Data Platform (CDP). Partitioning allows for efficient data organization and retrieval based on specific columns.

**In-place Table Evolution:**

This example demonstrates modifying an existing Iceberg table (`flights`) to add a new partitioning level by month within the existing year partition. The `ALTER TABLE` statement achieves this with minimal data movement, as the existing data remains indexed by year.

**Impala Querying and Partitioning Benefits:**

The example highlights the advantage of querying Iceberg tables with Impala, leveraging its performance capabilities. It showcases two queries, one targeting data partitioned by year only (2006) and another targeting data partitioned by year and month (2007).

- **EXPLAIN PLAN** is used to analyze the query execution strategy.
- The query targeting the year-month partition (2007) benefits from efficient scanning of a smaller data partition (month), leading to a significant performance improvement compared to the year-only partitioned data (2006).

**Key Takeaways:**

- Iceberg tables support in-place partition evolution.
- Partitioning by relevant columns optimizes query performance, especially when using Impala.

Remember to replace `${user_id}` with your actual user ID throughout the process.

   
Let's look at the file size. 

**In Impala**  
  
```SQL
SHOW FILES in ${user_id}_airlines_maint.flights;
```  

**For reference only**, you can check out other ways to run that command: [pyspark](documentation/IcebergLab-Documentation.md#pyspark)

Make a note of the average file size which should be around 5MB.
Also note the path and folder structure: a folder is a partition, a file is an ingest as we performed them above.

Now, let's alter the table, adding a partition on the month on top of the year.  

```SQL
ALTER TABLE ${user_id}_airlines_maint.flights SET PARTITION SPEC (year, month);
```  
  
Check the partition fields in the table properties
```SQL
DESCRIBE EXTENDED  ${user_id}_airlines_maint.flights
```

![Partitionkeys](./images/Partitionkeys.png)  


Ingest a month worth of data.  
  
```SQL
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv
 WHERE year <= 1996 AND month <= 1
```  

Let's have another look:   
  
```SQL
SHOW FILES in ${user_id}_airlines_maint.flights;
```  

Will show the newly ingested data, note the path, folder breakdown is different from before, with the additional partitioning over month taking place.


#### 2.3. Snapshots

From the INGEST queries earlier, snapshots was created and allow the time travel feature in Iceberg 
and this section will demonstrate the kind of operation you can run using that feature.

```SQL
DESCRIBE HISTORY ${user_id}_airlines_maint.flights;  
```  

Make a note of the timestamps for 2 different snapshots, as well as the snapshot id for one, you can then run:

```SQL
DESCRIBE HISTORY ${user_id}_airlines_maint.flights  BETWEEN '${Timestamp1}' AND '${Timestamp2}';
```
Timestamp format looks like this:  
`2024-04-11 09:48:07.654000000`  

You can run queries on the content of a snapshot refering its SnapshotID or timestamp:    
  
```SQL
SELECT COUNT(*) FROM ${user_id}_airlines_maint.flights FOR SYSTEM_VERSION AS OF ${snapshotid}
```
 or for example:  
 
```SQL
SELECT * FROM ${user_id}_airlines_maint.flights FOR SYSTEM_VERSION AS OF ${snapshotid}

```
Snapshot id format looks like:
`3916175409400584430` **with no quotes**

#### 3. ACID V2

Here we'll show the commands that could be run concomitantly thanks to [ACID](./documentation/IcebergLab-Documentation.md#acid) in Iceberg
Let's update a row.  


First, let's find a row:  

```SQL
SELECT year, month, tailnum, deptime, uniquecarrier  FROM  ${user_id}_airlines_maint.flights LIMIT 1
```
Save the values for year, month, tailnum and deptime to be able to identify that row after update.
Example:

![Savearow_hue-ACID](./images/Savearow_hue-ACID.png)  

You can bring back that row with a SELECT on a few unique characteristics and we'll update the uniquecarrier value to something else.

```SQL
SELECT * FROM ${user_id}_airlines_maint.flights
WHERE year = ${year}
  AND month = ${month}
  AND tailnum = '${tailnum}'
  AND deptime = ${deptime};
```


As Iceberg table are created as V1 by default, 
you will be able to migrate the table from Iceberg V1 to V2 using the below query:
```SQL
ALTER TABLE ${user_id}_airlines_maint.flights
SET TBLPROPERTIES('format-version'= '2')
```
Then try the UPDATE:

```SQL

UPDATE ${user_id}_airlines_maint.flights
SET uniquecarrier = 'BB' 
WHERE year = ${year}
  AND month = ${month}
  AND tailnum = '${tailnum}'
  AND deptime = ${deptime};

--- Check that the update worked:
SELECT * FROM ${user_id}_airlines_maint.flights
WHERE year = ${year}
  AND month = ${month}
  AND tailnum = '${tailnum}'
  AND deptime = ${deptime};
```

The following labs will take you through various aspects of CDP Public Cloud to enable you on what will be available to support Data Lakehouse use cases. 
CDP Public Cloud now includes support for Apache Iceberg in the following services: Impala, Hive, Flink, Sql Stream Builder (SSB), Spark 3, NiFi, and Replication Manager (BDR).
  
This makes Cloudera the only vendor to support Iceberg in a multi-hybrid cloud environment. 
Users can develop an Iceberg application once and deploy anywhere.  

