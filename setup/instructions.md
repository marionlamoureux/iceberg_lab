Save the airline data in a folder under the default CDP datalake bucket.
Make a note of the path to be indicated in the SQL under the variable "${data-bucket}"
Example: for s3://goes-se-sandbox01/airline-csv/flights/ the folder is airline-csv (no / before nor after)

In a RAZ enabled environment, make sure the admin user have access to the bucket under Ranger > S3.
Run the SQL queries saved under "code" to generate the tables.  

Nifi:  

Make the flow accessing in the CDF catalog for users to user. Download flow definition file here: 
https://github.com/cldr-steven-matison/NiFi-Templates/blob/main/SSBDemo.json  

Name the flow : SSB-Iceberg-Demo  

Kafka:  

Spin up a Streams Messaging Light Duty datahub, make sure it contains the following services.
Apache Kafka, Schema Registry, Streams Messaging Manager, Streams Replication Manager, Cruise Control