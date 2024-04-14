# Setup instructions
## Data Set

Save the airline data in a folder under the default CDP datalake bucket under several folders:  
-flights  
-airports  
-airlines  
-unique_tickets  
-planes  

Make a note of the path to be indicated in the SQL under the variable "${data-bucket}"
Example: for s3://goes-se-sandbox01/airline-csv/flights/ the folder is airline-csv (no / before nor after)

In a RAZ enabled environment, make sure the user group have access to the bucket under Ranger > "S3".
Also add the user group to Hadoop SQL > "all urls" and "all - database, table, column"
Run the SQL queries saved under "code" to generate the tables.  

## Nifi

Make the flow accessing in the CDF catalog for users. Download flow definition file here: 


Name the flow : SSB-Iceberg-Demo  

## Kafka

UPDATE RANGER.
You can check in the "Audit" the denialed requests and work from there.

Policies:
When you add the policies,  you need to add the Keycloak group associated to your env,   n ot the user(s).

They will be: hadoop sql, s3, kafka, schema Registry, Kudu, SOLR (without this one you may not see audit denials for rest)

Kafka: to Kafka Services, add the user group to policy "all topics." and user ssb to policy "connect internal - topic"
Solr : to Solr Services, add the user group to policy "all collections."

Spin up a Streams Messaging Light Duty datahub, make sure it contains the following services.
Apache Kafka, Schema Registry, Streams Messaging Manager, Streams Replication Manager, Cruise Control