
For reference only, and because Iceberg will integrate nicely with all the components of the Cloudera Data Platform 
and with different engines, the task can be performed in PySpark, looking like so:  

**In pyspark**  
  
```SQL
SELECT partition,file_path, file_size_in_bytes
FROM ${user_id}_airlines_maint.flights.files order by partition

```
