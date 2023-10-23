Save the airline data in a folder under the default CDP datalake bucket.
Make a note of the path to be indicated in the SQL under the variable "${data-bucket}"
Example: for s3://goes-se-sandbox01/airline-csv/flights/ the folder is airline-csv (no / before nor after)

In a RAZ enabled environment, make sure the admin user have access to the bucket under Ranger > S3.
Run the SQL queries saved under "code" to generate the tables.


