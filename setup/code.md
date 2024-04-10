Create the database from the files loaded in the S3 Bucket to load the data in a csv format for user to refer to when creating their own Iceberg databases (in CDP Public Cloud)


Make a note of the path to be indicated in the SQL under the variable "${data-bucket}"
Example: for s3://goes-se-sandbox01/airline-csv/flights/ the folder is **airline-csv** (no / before nor after)
*Note: I couldn't make the variable work for the path because of the single quotes, TODO: figure out how this works.*



```SQL
CREATE DATABASE airlines_csv;

drop table if exists airlines_csv.flights_csv;
CREATE EXTERNAL TABLE airlines_csv.flights_csv(month int, dayofmonth int, 
 dayofweek int, deptime int, crsdeptime int, arrtime int, 
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string, 
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, 
 depdelay int, origin string, dest string, distance int, taxiin int, 
 taxiout int, cancelled int, cancellationcode string, diverted string, 
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, 
lateaircraftdelay int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION '/<data_bucket>/flights' tblproperties("skip.header.line.count"="1");


drop table if exists airlines_csv.planes_csv;
CREATE EXTERNAL TABLE airlines_csv.planes_csv(tailnum string, owner_type string, manufacturer string, issue_date string, model string, status string, aircraft_type string, engine_type string, year int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION '/<data_bucket>/planes' tblproperties("skip.header.line.count"="1");

drop table if exists airlines_csv.airlines_csv;
CREATE EXTERNAL TABLE airlines_csv.airlines_csv(code string, description string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION '/<data_bucket>/airlines' tblproperties("skip.header.line.count"="1");

drop table if exists airlines_csv.airports_csv;
CREATE EXTERNAL TABLE airlines_csv.airports_csv(iata string, airport string, city string, state DOUBLE, country string, lat DOUBLE, lon DOUBLE) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION '/<data_bucket>/airports' tblproperties("skip.header.line.count"="1");
```

