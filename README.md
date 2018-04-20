# How to install it

Go to folder ***lenses_python*** and execute the follow command

`python3 setup.py install`

You must have already install *Python3* in your system

## How to run it

First we call it as we see below.

`from lenses_python.lenses import lenses`

`data=lenses(<url>,<username>,<password>)`

First we can use the below command to get the credentials and the roles which have our account

`data.GetCredentials()`

### Data Handler

- SQL Execute

  If we want to execute a Sql Query we should execute the following command

  `data.SqlHandler(<query>,<optional argument extract_pandas>, <optional datetimelist>, <optional formatinglist>)`

  If we don't put anything as extract_pandas the output of this is a JSON. If optional argument extract_pandas is different of zero , then return pandas data frame.

  In case we want output in in pandas data frames format, we can have the option to use the two optional arguments datetimelist and formatinglist.

  * ``datatimelist`` - is a list which contains the date of format of each element of this list.

  * ``formatinglist`` - is a list which contains the date of format of each element of list *datetimelist* . If all the formating of dates is same, we put only one in the list. For more info about the format check this page http://strftime.org .

    ​

    ​




## Topic Handler

* Get All Topics

​       If we want to take all topics,we execute the following command

`    data.GetAllTopics()`

​     The output of this is a list of  JSONs ,with all topics.

* List all names of topics

  `data.TopicsNames()`

  The output is a list of all names of topics.

* Info for specific topic

  `data.TopicInfo(<topic name>)`

  return a dictionary.

* Update topic configuration
  `data.UpdateTopicConfig(<topic name> , <optional configuration>, <optional filename>)`

  Example of configuration,

  ```
   {
     "configs": [{"key": "cleanup.policy","value": "compact"}]
                                                           }
  ```
  There are two options of parse the configuration. First we can set the argument of configuration as we see above. Or set configuration argument as empty and set argument filename with the name of file which content the configuration. Otherwise, set all arguments as empty, except filename. In file under section  ``Default`` set the the arguments ``topicname`` and ``config``.

  For example,

  ​

  `[Default]`

  `config:  {"configs": [{"key": "cleanup.policy","value": "compact"}] }`

  `topicname: "test"`

  ​

  ​

* Create new topic

  `data.CreateTopic(<topic name>, <replication>, <partitions>, <optional configuration> , <optional filename>)`

  Example of configuration,

  ```
  {
  "cleanup.policy": "compact",
  "compression.type": "snappy"
   }
  ```
  There are three options of parse the configuration. First we can set the argument configuration as we see above. Or set the configuration  argument as empty and set argument filename with the name of file which content the configuration. Otherwise, set all arguments as empty, except filename. In file under section ``Default`` set the arguments ``topicname`` and ``config`` .

  The structure of file will be like this,

  `[Default]`

                          config:{
                                          "configs": [
                                                          {"key": "cleanup.policy",
                                                                  "value": "compact"
                                                          }
                                                  ]
                                  }

* Delete Topic

  `data.DeleteTopic(<topic name>)`

  ​																																													

### Processor Handler

* Create new processor

  `data.CreateProcessor(<processor name>, <sql query>,<cluster name>, <optional namespace>, <optional pipeline>)`

  1. *processor name* :  string
  2. *sql query*: string
  3. *cluster name*: string
  4. *optional namespace* : string, applies for Kubernetes mode
  5. *optional pipeline*: string, applies for Kubernetes mode

  Return the LSQL id .

* Delete processor

  `data.DeleteProcessor(<processor name>)`

* Resume Processor

  `data.ResumeProcessor(<processor name>)`

* Pause Processor

  `data.PauseProcessor(<processor name>)`

* Update processor runners

  `data.UpdateProcessorRunners(<processor name>, <number of runners>)`

### Schemas Handler

* Get name of all subjects

  `data.GetAllSubjects()`

* List the versions of subjects

  `data.ListVersionsSubj(<name of subject>)`

* Get schema by id

  `data.GetSchemaById(<subject's id>)`

* Get schema by version

  `data.GetShcemaByVer(<name of subject>, <number of version>)`

* Register new schema

  `data.RegisterNewSchema(<name of schema>, <optional schema configurations>, <optional filename>)`

  Example of schema configuration,

  ```
  {'schema': '{"type":"record",
               "name":"reddit_post_key",
               "namespace":"com.landoop.social.reddit.post.key",
               "fields":[{"name":"subreddit_id","type":"string"}]}'
               }
  ```

  Return a dictionary with id of new schema,

  ```
  {'id': <id>}
  ```
  There are three options of parse the schema configuration. First we can set the argument schema configuration as with see above. Or set schema configuration argument as
  empty and set argument filename with the name of file which content the schema configuration. Otherwise, set all arguments empty , except filename. In file under section

  ``Default`` set arguments ``schema`` and ``config``.

  ​

* Get Global Compatibility

  `data.GetGlobalCompatibility()`

* Get compatibility of specific subject

  `data.GetCompatibility(<subject's name>)`

* Delete specific subject

  `data.DeleteSubj(<name of subject>)`

* Delete schema by version

  `data.DeleteSchemaByVersion(<name of subject>, <version of subject>)`

* Change compatibility of subject

  `data.ChangeCompatibility(<name of subject>, <compatibility>)`

  Example of compatibility,

  ```
  {'compatibility': 'BACKWARD'}
  ```

* Update Global compatibility

  `data.UpdateGlobalCompatibility(<compatibility>)`

  Example of compatibility,

  ```
  {'compatibility': 'BACKWARD'}
  ```

### Connector Handler

* Listed all connectors

  `data.ListAllConnectors(<name of cluster>)`

* Get info about specific connector

  `data.GetInfoConnector(<name of cluster>, <name of connector>)`

* Get connector configuration

  `data.GetConnectorConfig(<name of cluster>, <name of connector>)`

* Get connector status

  `data.GetConnectorStatus(<name of cluster>, <name of connector>)`

* Get connector tasks

  `data.GetConnectorTasks(<name of cluster>, <name of connector>)`

* Get status of specific task

  `data.GetStatusTask(<name of cluster>, <name of connector>, <task's id>)`

* Restart connector task

  `data.RestartConnectorTask(<name of cluster>, <name of connector>,<task's id>)`

* Create new connector

  `data.CreateConnector(<name of cluster>, <optional configuration>, <optional filename>)`

  Example of file/dictionary,

  ```
  {'config': {'connect.coap.kcql': '1',
   'connector.class': 'com.datamountaineer.streamreactor.connect.coap.sink.CoapSinkConnector'},
  'name': 'name'}
  ```

* Set connector configuration

  `data.SetConnectorConfig(<name of cluster>,<name of connector>,<optional configuration>, <optional filename>)`

  Example of configuration file,

  ```
  {'connector.class': 'org.apache.kafka.connect.file.FileStreamSinkConnector',
                              'task.max': 5,
                              'topics': 'nyc_yellow_taxi_trip_data,reddit_posts,sea_vessel_position_reports,
                              telecom_italia_data',
                              'file': '/dev/null',
                              'tasks.max': '4',
                              'name': 'nullsink'}
  ```
  There are three options of parse the arguments. First we can set the argument configuration as we see above. Or set configuration argument as
  empty and set argument filename with the name of file which content the configuration. Otherwise, set all arguments as empty, except filename.In file
  under section ``Default`` set arguments ``cluster``, ``connector`` and ``config``.

* Delete connector

  `data.DeleteConnector(<name of cluster>, <name of connector>)`



### Web Socket Handler

* Subscribe. A client can SUBSCRIBE to a topic via **SQL**. 

  `data.SubscribeHandler(<url>, <client id>, <sql query>, <write>, <filename>, print_results>)`

   * write is boolean parameter and is pre-defined as **False**. If define it as **True** , values of data save in file, which name is defined by **filename** parameter.
   * filename is parameter which must give a name of file where logs are saved.
   * print_results is boolean parameter and is pre-defined as **True**. When it's **True** print the incoming data , otherwise (if it's **False**) it doesn't print anything.

  ​

* Publish. A client can PUBLISH messages to a topic. The current version supports only string/json. In the future, we will add support for Avro.

  `data.Publish(<url>, <client id>, <topic>, <key>, <value>)`

* Unscribe. A client can UNSUBSCRIBE from a topic.

  `data.Unscribe(<url>, <client id>, <topic>)`

* Commit. A client can COMMIT the (topic, partition) offsets

  `data.Commit(<url>, <client id>, <topic>, <partition>, <offset>)`

  ​

### ACLs Handler

* Create/Update ACLs

  `data.SetAcl(<resourceType>,<resourceName>,<principal>,<permissionType>,<host>, <operation>)`

   * `resourceType`, string, required
  * `resourceName`, string, required
  * `principal`, string, required
  * `permissionType`, string, required (either Allow or Deny)
  * `host`, string, required
  * `operation`, string, required

  Example,

  `data.SetAcl("Topic","transactions","GROUPA:UserA","Allow","*","Read")`

* Get ACLs

  `data.GetACL()`



### Quota Handler

* Get Quotas

  `data.GetQuotas()`

  Return a list of dictionaries.

* Create/Update Quota - All Users

  `data.SetQuotasAllUsers(config)`

  * `config` The quota contraints.

    Example of config,

    ```
    {
        "producer_byte_rate" : "100000",
        "consumer_byte_rate" : "200000",
        "request_percentage" : "75"
    }
    ```

* Create/Update Quota - User all Clients

  `data.SetQuotaUserAllClients(user, config)`

  Where ,

  - `user` The user to set the quota for

  - `config` The quota contraints

    Example of config,

    ```
    {
        "producer_byte_rate" : "100000",
        "consumer_byte_rate" : "200000",
        "request_percentage" : "75"
    }
    ```

* Create/Update a Quota - User/Client pair

  `data.SetQuotaUserClient(user, clientid, config)`

   Where,

  - `user` The user to set the quota for

  - `client-id` The client id to set the quota for

  - `config` The quota contraints

    Example of config,

    ```
    {
        "producer_byte_rate" : "100000",
        "consumer_byte_rate" : "200000",
        "request_percentage" : "75"
    }
    ```

* Create/Update a Quota - User

  `data.SetQuotaUser(user, config)`

  Where, 

  ​           

  - `user` The user to set the quota for

  - `config` The quota contraints

    Example of config,

    ```
    {
        "producer_byte_rate" : "100000",
        "consumer_byte_rate" : "200000",
        "request_percentage" : "75"
    } 
    ```

* Create/Update Quota - All Clients

  `data.SetQuotaAllClient(config)`

  Where ,

  - `config` The quota contraints

    Example of config,

    ```
    {
        "producer_byte_rate" : "100000",
        "consumer_byte_rate" : "200000",
        "request_percentage" : "75"
    }
    ```

* Create/Update a Quota - Client

  `data.SetQuotaClient(clientid, config)`

  Where,

  - `client-id` The client id to set the quota for
  - `config` The quota contraints

     Example of config,

  ```
  {
      "producer_byte_rate" : "100000",
      "consumer_byte_rate" : "200000",
      "request_percentage" : "75"
  }  
  ```

* Delete Quota - All Users

  `data.DeleteQutaAllUsers(config)`

  Where,

  * `config`: A list we the parameters which want to delete. For example,

    `config=["producer_byte_rate","consumer_byte_rate"]`

* Delete Quota - User all Clients

  `data. DeleteQuotaUserAllClients(user, config)`

  Where,

  - `user` The user to set the quota for

  - `config`: A list we the parameters which want to delete. For example,

    `config=["producer_byte_rate","consumer_byte_rate"]`

* Delete a Quota - User/Client pair

  `data.DeleteQuotaUserClient(user, clientid, config)`

  Where,

  - `user` The user to set the quota for

  - `clientid` The client id to set the quota for

  - `config`: A list we the parameters which want to delete. For example,

    `config=["producer_byte_rate","consumer_byte_rate"]`

* Delete a Quota - User

  `data.DeleteQuotaUser(user, config)`

  Where,

  - `user` The user to set the quota for

  - `config`: A list we the parameters which want to delete. For example,

    `config=["producer_byte_rate","consumer_byte_rate"]`

* Delete Quota - All Clients

  `data.DeleteQuotaAllClients(config)`

  Where,

  	*  `config`: A list we the parameters which want to delete. For example,

  `config=["producer_byte_rate","consumer_byte_rate"]`

* Delete a Quota - Client

  `data.DeleteQuotaClient(clientid, config)`

  Where,

  - `clientid` The client id to set the quota for
  - config`: A list we the parameters which want to delete. For example,

  `config=["producer_byte_rate","consumer_byte_rate"]`

  ### 

