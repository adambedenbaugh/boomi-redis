

# Boomi Redis Connector

I wanted to share a solution I recently developed to have high-throughput in the Dell Boomi AtomSphere Platform: use of Cache Pattern with In-Memory Data Grid or In-Memory Database (cf [IMDG/IMDB](https://en.wikipedia.org/wiki/List_of_in-memory_databases). 
The Connector is designed to allow the use of a Caching/In-Memory Data Grid/Database in Boomi and thus, provide high-throughput APIs and Processes which stored information in Memory and avoid unnecessary calls to the backend system for read/query scenarios.

Boomi Cache Connector is a Generic Connector which will allows you to connect to any Cache system. Initially, it is supporting **Ehcache**,  **Redis** (Standalone or Clustered) and **memcached**.

![Alt text](resources/BoomiCache_Connector.png?raw=true "BoomiCache")

The Boomi Cache Connector can accelerate: 

- ![Alt text](resources/API.png?raw=true "BoomiCache") **RESTFul APIs**, the Connector can **automatically calculate** the caching key based on the HTTP queries and parameters provided by the API Consumer.
- ![Alt text](resources/Process.png?raw=true "BoomiCache") **Integration Processes**, you will provide the key and the value to store.


The Cache Connector will access the Cache system, the **Cache** system will contains several **CacheObjects**. And each **CacheObject** contains a **list of key-value Pair**. The key is a String and the Value can be any Object (String, JSON, XML...)


## Getting Started

Please download the library [connector-archive](BoomiCacheConnector-0.79.zip?raw=true) and the connector descriptor [connector-descriptor](connector-descriptor.xml?raw=true)

### Prerequisites in Boomi

#### Setup of the connector

Please go to Setup>Account>Publisher and fill out the information.

![Alt text](resources/Publisher.png?raw=true "BoomiCache")

And then, go to Setup>Development Resources>Developer and create a new Group by clicking on ![Alt text](resources/Boomi_Developer_Connector_Init.png?raw=true "BoomiCache"). On Initial Connector Display Name, you can put Cache Connector or Cache Connector (Beta). The two files to upload are the files you previous downloaded. For the Vendor Product Version, please mentioned the version of the Zip Archive.

![Alt text](resources/Boomi_Developer_Connector.png?raw=true "BoomiCache")

The result should like that:

![Alt text](resources/Boomi_Developer_Connector_Done.png?raw=true "BoomiCache")


#### Use of the Cache Connector

The configuration of the Cache is done in the Connector:

![Alt text](resources/BoomiCache_Connector_Config.png?raw=true "BoomiCache")

**Three operations** are provided

- Get: Get information from the Cache system based on a key (can be calculated for APIs)
- Upsert: Create or Update information in the Cache system, input is key-value pair
- Delete: Delete a key-value pair based on a key or delete the full Cache Object

Please use the "Browse" Option in the Connector to Generate the Request/Response Profile. You can use Test Atom Cloud or your On-Prem Atom. 

#### Example of RESTFul API 
The Following Process is a Web Services Server process getting information from a connector

![Alt text](resources/Boomi_Process_NoCache.png?raw=true "BoomiCache")

We will update this process to add **Caching Shapes** with the Caching logics:

![Alt text](resources/Boomi_Process_BoomiCacheConnector.png?raw=true "BoomiCache")

In the try path, we are using **Get** operation, the **key is automatically computed** and the **value** returned will be the data in the cache.

![Alt text](resources/Boomi_Op_Get.png?raw=true "BoomiCache")

In case of cache miss, an exception with be thrown and the process will go to the Catch Path.

We are calling the back-end system and store (**Upsert**) the value to the Cache, again the **key is automatically computed** 

![Alt text](resources/Boomi_Op_Upsert.png?raw=true "BoomiCache")

We are not using the **Delete** operation here but it is very similar to **Get** operation and is taking a Key, it you put * as key, it will delete the full Cache object.

### Automatic key calculation
When using RESTFul APIs, you can enable the **Automatic Key Computation** in the operation and avoid providing the key (just put 'auto' in the ID). The connector will use the HTTP queries and params.
By default all dynamic process properties starting with *query_* and *param_* will be used, to change it, set the property in the operation to set the regular expression to filter the dynamic process properties:


## Example of APIs call

### Call of API with JSON results

The following API calls will store some values in Redis using the automatic key generation.
![Alt text](resources/Boomi_API_Call.png?raw=true "BoomiCache")

![Alt text](resources/Boomi_API_Call_2.png?raw=true "BoomiCache")

![Alt text](resources/Boomi_API_Call_3.png?raw=true "BoomiCache")

![Alt text](resources/Boomi_API_Call_4.png?raw=true "BoomiCache")

### Data stored in Redis

You can see the **keys** generated when we are using HTTP Parameters:

![Alt text](resources/Boomi_API_Redis.png?raw=true "BoomiCache")


## Use of Azure Redis Caches

You can use Boomi Connector to connect to an On-Prem Redis and also to Secured Cloud one. On Azure, please select "Redis Caches", select the option of your instance (the Cache Region should be the same as the VM hosting Boomi to minimize latency.
Once all the options are selected, click on "Create Redis Cache"

![Alt text](resources/Azure_Redis_0.png?raw=true "BoomiCache")

Copy the Host name:

![Alt text](resources/Azure_Redis_1.png?raw=true "BoomiCache")

Copy the primary key:

![Alt text](resources/Azure_Redis_2.png?raw=true "BoomiCache")

Paste the Hostname followed by :6380 in the Connector Configuration and paste the Key to the Password Value, please don't forget to check "Use SSL".

For additional security, you can also update the Redis firewall to allow only your VMs (and your local network) to access the instance. 

## Use of Redis on Atom Cloud

Pooling need to be disable to make the Redis connector working on the Boomi Public Runtime Cloud. 



## Installation
1. Navigate to [boomi-redis releases](https://github.com/adambedenbaugh/boomi-redis/releases) and download the latest .zip file and connector-description.xml file.
2. Import into Boomi by navigating to Settings -> Develper.
3. Upload the .zip file and connector-descpition.xml file.
4. Update the connector icon with Postman collection on initial install. 


## Updating Custom Connector Icon

The icon for the custom connector can be updated with the [ConnectorIcon API](https://developer.boomi.com/docs/api/connectors/ConnectorIcon#tag/Connector-Icon/operation/CreateConnectorIcon).

1. Open the `assets/postman/boomi_redis_connector.postman_collection.json` file within Postman.
2. Within the imported collection, update the Basic Username and Password within the Authorization tab. The username should begin with `BOOMI_TOKEN.`.
![](assets/postman-auth.png)
3. Navigate to the Variables tab and update `classificationType` and `baseUrlConnector`. Values to be set are below. 
    - `classificationType`: classificationType is the value of the Type column of the Boomi Enterprise Platform > Developer tab > Connectors table. Example: `accountID-sample-uat`.
    - `baseUrlConnector`: `https://api.boomi.com/connector/api/rest/v1/{account-id}` Update the account-id with the Boomi Account Id. 
    ![](assets/postman-variables.png)
4. Under the request's body add `postman/redis.svg` to the `connectorIcon` value.
![](assets/postman-body.png)
5. Click Send on the Request.
6. Clear the browser cache each time the above request is executed to view the new icon. 



## TODO

- Add connection timeouts
- Enabled pooling



This repo is a fork of [BoomiCacheConnector](https://bitbucket.org/officialboomi/boomicacheconnector/src/master/).