
**Handy Iceberg Links**  
[Apache Iceberg Documentation (be careful not everything may be supported yet in CDP)](https://iceberg.apache.org/docs/latest/)  
[Impala Iceberg Cheatsheet](https://docs.google.com/document/d/1cusHyLBA7hS5zLV0vVctymoEbUviJi4aT8SfKyIe_Ao/edit?usp=drive_link)  


# Data Set
Schema for the data set is below: Entity-Relation Diagram of tables we use in todays workshop:

Fact table: flights (86mio rows)
Dimension tables: airlines (1.5k rows), airports (3.3k rows), planes (5k rows) and unique tickets (100k rows).

**Dataset airlines schema**  

![Airlines schema](../images/Iceberg_airlinesschema.png)

**Raw format of the flight set**  

Here displayed in a file explorer:

![Raw Data Set](../images/dataset_folder.png)



# PySpark

For reference only, and because Iceberg will integrate nicely with all the components of the Cloudera Data Platform 
and with different engines, the task can be performed in PySpark, looking like so:  

**In pyspark**  
  
```SQL
SELECT partition,file_path, file_size_in_bytes
FROM ${user_id}_airlines_maint.flights.files order by partition

```
# ACID

ACID is an acronym that refers to the set of 4 key properties that define a transaction: 
Atomicity, Consistency, Isolation, and Durability. If a database operation has these ACID properties, 
it can be called an ACID transaction, and data storage systems that apply these operations are called transactional systems.
This set of properties of database transactions intended to guarantee data validity despite errors, power failures, and other mishaps.

In the context of Iceberg specifically, additional information can be found in this [blog article](https://blog.min.io/iceberg-acid-transactions/)


# CDP environments

In CDP, an environment is a logical subset of your cloud provider account including a specific virtual private network. 
You can register as many environments as you require, each living either on prem, in the cloud, in as many supporter cloud providers as you need (out of AWS, Azure, GCP


The “environment” concept of CDP is closely related to the virtual private network in your cloud provider account. 
Registering an environment provides CDP with access to your cloud provider account and identifies the resources in your cloud provider account 
that CDP services can access or provision. 
A single environment is contained within a single cloud provider region,
so all resources deployed by CDP are deployed within that region within one specific virtual network. 
Once you’ve registered an environment in CDP, 
you can start provisioning CDP resources such as clusters, which run on the physical infrastructure in an CSP data center.

# Workload Password

To access non-SSO interfaces, each user and machine user must set a workload password (also known as "FreeIPA password"). An administrator can set other users' workload passwords.
Set your own workload password
As a CDP user, you can see on your profile page if you have previously set your workload password and if the password is about to expire. There are two cases when you may want to set your workload password:
When you first start using CDP.
When your password expires. This may or may not happen depending on your company's policies. If your password does expire, you will see a banner notification on the CDP web interface 10 days before the expiry date.
You can also see on your user’s profile page the state of your workload password (if it expires soon or cannot yet be changed).


# Row Level Operations

Depending on the COW or MOR setting, **Hive** performs updates and deletes to Iceberg tables as follows:
- COW: Hive creates a new version of files for each update/delete. Use COW when updating/deleting a large number of rows, or when reading data frequently.
- MOR (default): Updates/deletes are logged to delta files, which tends to be faster than creating new versions of the files. Later a compaction can eliminate the delete files and rewrite the affected data files.
Impala uses only the MOR method. Impala does not support copy-on-write and will fail if configured for copy-on-write. Impala does support reading copy on write tables.

**When to use COW or MOR**
Set either COW or MOR based on your use case and rate of data change. Consider the following advantages and disadvantages of the modes:  
  
**MOR**
- Writes are efficient.
- Reads are inefficient due to read amplification, but regularly scheduled compaction can reduce inefficiency.
- A good choice when streaming.
- A good choice when frequently writing or updating, such as running hourly batch jobs.
- A good choice when the percentage of data change is low.
  
**COW**
- Reads are efficient.
- A good choice for bulk updates and deletes, such as running a daily batch job.
- Writes are inefficient due to write amplification, but the need for compaction is reduced.
- A good choice when the percentage of data change is high.



# Expiring Snapshots

You should periodically expire snapshots to delete data files that are no longer needed, and reduce the size of table metadata.

Each write to an Iceberg table creates a new snapshot, or version, of a table. 
You can use snapshots for time-travel queries, or to roll back a table to a valid snapshot. 
Snapshots accumulate until they are expired by the expire_snapshots operation.


# Data hubs

ata Hub is a service for launching and managing workload clusters powered by Cloudera Runtime 
(Cloudera’s unified open source distribution including the best of CDH and HDP)

Data Hub includes a set of cloud optimized built-in templates for common workload types,
as well as a set of options allowing for extensive customization based on your enterprise’s needs. 
Furthermore, it offers a set of convenient cluster management options such as cluster scaling, stop, restart, terminate, and more. All clusters are secured via wire encryption and strong authentication out of the box, and users can access cluster UIs and endpoints through a secure gateway powered by Apache Knox. Access to S3 cloud storage from Data Hub clusters is enabled by default (S3Guard is enabled and required in Runtime versions older than 7.2.2).

Data Hub provides complete workload isolation and full elasticity so that every workload, every application,
or every department can have their own cluster with a different version of the software, different configuration, and running on different infrastructure. This enables a more agile development process.

Since Data Hub clusters are easy to launch and their lifecycle can be automated, you can create them on demand and when you don’t need them,
you can return the resources to the cloud.

*In this lab*, the Kafka instance is hosted in a datahub, directly deployed from the catalog of images 
that comes out of the box in your CDP platform, configured and set up to handle Stream messages worklods. The Template name is `Streams Messaging Light Duty: Apache Kafka, Schema Registry, Streams Messaging Manager, Streams Replication Manager, Cruise Control`
The SSB instance is also hosted in a datahub, template name: `Streaming Analytics Light Duty with Apache Flink`

# Hive Metastore URI

The hive metastore for the datalake is indicated in a configuration file which
can be downloaded from Cloudera Manager:
  
Access the Management Console:  

![AccessManagementConsole.png](../images/AccessManagementConsole.png)  


Select the Datalake:  

![Datalake.png](../images/Datalake.png)  


Access the url for the Cloudera Manager of the environment:  

![ClouderaManagerinfo.png](../images/ClouderaManagerinfo.png)  


Access the Hive metastore service in Cloudera Manager: 

![AccessHiveMetastoreservice.png](../images/AccessHiveMetastoreservice.png)  

Download the Configuration files in a zip:  

![DowloadHiveconfigurationzip.png](../images/DowloadHiveconfigurationzip.png)

In the hive-conf.xml file, grab the value for the hive.metastore.uris
![hive-conf.png](../images/hive-conf.png)  

Hive Metastore URI example:

`thrift://workshop-aw-dl-master0.workshop.vayb-xokg.cloudera.site:9083`


# Kafka Broker endpoints

In CDP Public Cloud, Kafka is deployed in a [Datahub](IcebergLab-Documentation.md#data-hubs), which is a step previously setup by the lab admin.

![Datahubs](../images/AccessDataHub.png)  

The name of the Datahub to access will be provided by the instructor.

The Kafka broker endpoints are available on the overview page of the Datahub,on the bottom menu, under "Endpoints".

Kafka Endpoints in Datahub overview
![Kafka Borker Endpoints](../images/Iceberg_KafkaBorkerEndpoints.png)

# SSB Project

Created or imported projects can be shared with other users in Streaming SQL Console. You can invite members
using their Streaming SQL Console username and set the access level to member or administrator.
Projects aim to provide a Software Development Lifecycle (SDLC) for streaming applications in SQL Stream Builder
(SSB): they allow developers to think about a task they want to solve using SSB, and collect all related resources,
such as job and table definitions or data sources in a central place.
  
A project is a collection of resources, static definitions of data sources, jobs with materialized views, virtual tables,
user-defined functions (UDF), and materialized view API keys. These resources are called internal to a project and
can be safely used by any job within the project.

A project can be set up by importing a repository from a github source, which we will do here. Within the Git repository, "Project" would be pointing to a folder
within the github repository containing the files to set up data sources, api keys and jobs within SSB. As this folder name needs to be unique, the hack for this workshop
is that all attendees are pointing to the same git repository but pointing to pre-created folders within it named after their username.  
  
  
  
# SSB environment
 Creating an environment file for a project means that users can create a template with variables that could be used to
store environment-specific configuration.
For example, you might have a development, staging and production environment, each containing different clusters,
databases, service URLs and authentication methods. Projects and environments allow you to write the logic and create the resources once, and use template placeholders for values that need to be replaced with the environment
specific parameters.
To each project, you can create multiple environments, but only one can be active at a time for a project.
Environments can be exported to files, and can be imported again to be used for another project, or on another cluster.
While environments are applied to a given project, they are not part of the project. They are not synchronized to Git
when exporting or importing the project. This separation is what allows the storing of environment-specific values, or
configurations that you do not want to expose to the Git repository.


# Schemas in SSB


When you select Data Format as AVRO, you must provide the correct Schema Definition when creating the table for SSB to be able to successfully process the topic data. For JSON tables, though, SSB can look at the data flowing through the topic and try to infer the schema automatically, which is quite handy at times. Obviously, there must be data in the topic already for this feature to work correctly.

Note: SSB tries its best to infer the schema correctly, but this is not always possible and sometimes data types are inferred incorrectly. You should always review the inferred schemas to check if it’s correctly inferred and make the necessary adjustments.
