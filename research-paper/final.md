# Neo4j Fabric
## Abstract
* Neo4j is a popular graph database that provides developers and data scientists with the most trusted and sophisticated tools for rapidly building today's intelligent applications and smart workflows. Neo4j is rapidly being used as the foundation for production analytic and machine learning use cases, such as fraud detection and real-time recommendations. In terms of scalability, Neo4j has a few downsides. Neo4j fabric is a new functionality offered to store and retrieve data across various databases.

## Table of contents

**I. Graph Database**

**II. Drawbacks of Neo4j**

**III. Data Sharding**
     
     3.1 Sharding uses
     3.2 Advantages of sharding
     3.3 Disadvantages of sharding
 
**IV. Neo4j Fabric**

     4.1 Fabric structure
       4.1.1 Advantages of sharding
       4.1.2 Fabric graph
     4.2 Architecting a system with fabric
 
 
**V. Sources**

### **I. Graph Database**
A graph database is a platform specialized to the creation and management of graphs. Graphs employ nodes, edges, and properties to represent and store data in ways that relational databases cannot.
### **II. Drawbacks of Neo4j**
Neo4j database has a hard limit in terms of scalability since it essentially was restricted to running on a single server. With the release of Neo4j 4.0, it added a graph sharding technique called Fabric to facilitate data distribution across instances or systems. It allows you to access data in individual graphs as well as a linked graph that spans many partitions/databases.
### **III. Data Sharding**
Data sharding is the process of breaking data into horizontal partitions and organizing them into several instances or servers, usually to distribute load across many access points. When needed, these shards can be retrieved separately or aggregated to show all the data.

<img src="https://media.geeksforgeeks.org/wp-content/cdn-uploads/20210312174820/Sharding.jpg" width="1000"/>

#### *3.1 Sharding Uses*
Sharding into independent databases or instances makes sense in a few different scenarios. Many of them are related to business contexts and data volume. We can have a look at a couple of the ones listed below.
* You may need to store some data in isolated databases to protect or limit access for legal or privacy reasons (i.e. GDPR laws).
* For everyday business, data is already compartmentalized into various instances, but you may need to tie them together into a cohesive graph (i.e. knowledge graph).
* To reduce query latency in different regions, relevant graph segments can be stored in cloud regions adjacent to query request sources (i.e. application hosted in Europe that queries for data stored in a Europe cloud region).
* Although archived data may be divided by date, such as year, adhoc reporting or other requirements may need queries across these graphs.
*	As the network grows (tens of billions of nodes), it becomes sense to split the data into smaller graphs that can run on smaller hardware and be accessible by the right people.
There are additional cases in which sharding the data is the best option, but at its core, sharding is a method of distributing data among instances or geographic regions for simpler or more secure access.
#### *3.2 Advantages of Sharding*
By delivering higher read/write speed, storage capacity, and high availability, sharding allows you to grow your database to accommodate increased load to an almost infinite degree. Let's take a closer look at each of these points.
* Increased Read/Write Throughput — If read and write operations are constrained to a single shard, the capacity of both read and write operations is boosted by splitting the dataset across numerous shards.
* Increased Storage Capacity — Similarly, increasing the number of shards allows for near-infinite scalability by increasing overall total storage capacity.
* High Availability — Finally, there are two ways that shards provide high availability. Every piece of data is copied since each shard is a replica set. Second, because the data is dispersed, even if an entire shard goes down, the database remains partially functional, with separate shards hosting different parts of the schema.
#### *3.3 Disadvantages of Sharding*
Sharding has several disadvantages, including overhead in query result compilation, administration complexity, lack of native support, and increased infrastructure expenses.
### **IV. Neo4j Fabric**
Fabric in Neo4j is a technology that allows you to store and retrieve data from various databases. With this feature, you can query data in the same or many DBMSs with a single Cypher query.
Fabric provides infrastructure and tooling for:
* Data Federation: the ability to use unconnected graphs to retrieve data from multiple sources.
* Data Sharding: utilizing a shared graph partitioned over many databases to obtain data from diverse sources.
#### *4.1 Fabric structure*
##### *4.1.1 Fabric Database:*
A Fabric virtual database is included in a Fabric setup, and it acts as a gateway to a federated or sharded graph architecture. For multi-graph queries, this database serves as the execution context. By identifying the Fabric execution context as the session's preferred database, drivers and client applications have access to and utilize it.
The Fabric virtual database (execution context) differs from typical databases in that it does not store data and instead relays information from other sources. The Fabric virtual database can only be configured using a standalone Neo4j DBMS.
##### *4.1.2 Fabric Graph:*
A Fabric virtual database's data is organized as graphs. Client programs see graphs as local logical structures that store data in one or more databases.

<img src="https://dist.neo4j.com/wp-content/uploads/concept_fabric_example_system.jpg" width="900"/>

#### *4.2 Architecting a system with Fabric*
The people data sharded graph system may be built in a variety of ways, notably with multi-database and clustering capabilities. Graphs might be stored in the same DBMS on a physical server at a regional location, or they could be spread among several DBMSs on physical and cloud servers throughout the world.
Because this might be confusing, especially if you're new to the notion of sharding, we'll simply go through three designs (out of many) that can be used in a variety of scenarios.

Example 1: A single DBMS for everything

This database management system (DBMS) might be hosted locally or remotely, on in-house or cloud servers. We've sharded our data into one instance for the fabric database and six unique instances for each continent's people data, regardless of the DBMS's location.
There is a tolerable amount of traffic for a single DBMS to handle, latency has little to no influence on, and there are no legal or data privacy problems with storing the domain together, among other reasons for architecting the system this way.

<img src="https://dist.neo4j.com/wp-content/uploads/concept_fabric_example_1dbms.jpg" width="800"/>

Example 2: Fabric database in separate DBMS

We can take our previous example a step further by putting the fabric database in its own database management system. Our proxies (fabric DB) or data instances (people data) may now be local or remote, on-premises or in the cloud. These decisions are based on the needs and preferences of the many people involved. Our shards are still divided between one for the fabric database and six for the continent-specific individual’s data.
The necessity to load-balance requests is one of the reasons we could adopt this design. We'll need to duplicate the data across regions to do this. Fabric databases, on the other hand, must be freestanding, single instances. We can then place all of the data instances into another DBMS that can be added to a cluster for replication by placing that instance into its own DBMS. Fabric's single instance might also be replicated to handle increased demand on the clusters.

<img src="https://dist.neo4j.com/wp-content/uploads/concept_fabric_example_2dbms.jpg" width="800"/>

Example 3: Multiple DBMSs

To meet company objectives and requirements, each DBMS may be utilized in any mix of local and remote, in-house or cloud. The shards are still divided into one for the coordinator (fabric database) and six for the continent-based persons data, but all these instances are now packaged or segregated into independent systems.

<img src="https://dist.neo4j.com/wp-content/uploads/concept_fabric_example_3dbms.jpg" width="800"/>


This architecture may be used because certain data must be hosted privately or independently from other data, or because many requests are for a single dataset. It might also be for faster requests to and from specific locations.

### **V. Sourcess**

* https://neo4j.com/developer/neo4j-fabric-sharding/
* https://adamcowley.co.uk/neo4j/sharding-neo4j-4.0/
* https://www.analyticsvidhya.com/blog/2022/01/a-comprehensive-guide-on-neo4j-graph-database/
* https://www.datanami.com/2020/02/04/neo4j-going-distributed-with-graph-database/
* https://www.mongodb.com/features/database-sharding-explained/
* https://www.oracle.com/autonomous-database/what-is-graph-database/
* https://neo4j.com/blog/getting-started-with-neo4j-fabric/
* https://www.geeksforgeeks.org/database-sharding-a-system-design-concept/
* https://neo4j.com/docs/operations-manual/current/fabric/introduction/
