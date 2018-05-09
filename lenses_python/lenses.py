from requests import *
from json import *

from lenses_python.SqlHandler import SqlHandler as SqlH
from lenses_python.TopicHandler import TopicHandler as TopicH
from lenses_python.ProcessorHandler import ProcessorHandler as PrcH
from lenses_python.SchemasHandler import SchemasHandler as SchemaH
from lenses_python.ConnectorHandler import ConnectorHandler as ConnH
from lenses_python.WebSocketHandler import SubscribeHandler as SubSH
from lenses_python.PublishHandler import PublishHandler as PuHandl
from lenses_python.ACLHandler import ACLHandler
from lenses_python.QuotaHandler import QuotaHandler


class lenses:

    def __init__(self, url, username, password):
        """

        :param url:
        :param username:
        :param password:
        """
        self.url = url
        self.username = username
        self.password = password
        self._Connect()

    def _Connect(self):
        """

        :return:Token
        """
        LOGIN = "/api/login"
        login_url = self.url+LOGIN
        payload = {'user': self.username,
                   'password': self.password}
        default_headers = {'Content-Type': 'application/json',
                           'Accept': 'application/json'}
        response = post(login_url, data=dumps(payload), headers=default_headers)
        if response.status_code != 200:
            raise Exception("Could not connect to the API [{}]. Status code [{}]. Reason [{}]"
                            .format(login_url, response.status_code, response.reason))
        else:
            self.token = response.json().get("token", None)
            if self.token == None:
                raise Exception("Cannot recieve Token.")
        self.credentials = response.json()

    def GetCredentials(self):
        return self.credentials

    # Sql Handler

    def SqlHandler(self, query, extract_pandas=0, datetimelist=[], formatinglist=[]):
        """

        :param query:
        :param extract_pandas: if is 0 return dict else return pandas dataframes
        :param datetimelist: List of keys which content datetime string
        :param formatinglist: List of formation of elements of datetimelist keys
        :return:The result of the given query
        """
        return SqlH(self.url, self.username, self.password, self.token, query, datetimelist, formatinglist).ExecuteSqlQuery(extract_pandas)

    # Topics Handler

    def GetAllTopics(self):
        """

        :return:All topics with their info
        """
        return TopicH(self.url, self.username, self.password, self.token).GetAllTopics()

    def TopicInfo(self, topicname):
        """

        :param topicname: Then name of topic
        :return: Info for specific topic , as dictionary
        """
        return TopicH(self.url, self.username, self.password, self.token).TopicInfo(topicname)

    def TopicsNames(self):
        """

        :return: A list with all topic names
        """
        return TopicH(self.url, self.username, self.password, self.token).LstOfTopicsNames()

    def UpdateTopicConfig(self, topicname, config="", filename=""):
        """

        :param topicname:
        :param config:
        :param filename:
        :return:
        """
        return TopicH(self.url, self.username, self.password, self.token).UpdateTopicConfig(topicname, config, filename)

    def CreateTopic(self, topicName, replication, partitions, config="", filename=""):
        """

        :param topicName:
        :param replication:
        :param partitions:
        :param config:
        :param filename:
        :return:
        """
        return TopicH(self.url, self.username, self.password, self.token).CreateTopic(topicName, replication,
                                                                                      partitions, config, filename)
    def DeleteTopic(self, topicname):
        """

        :param topicname:
        :return:
        """
        return TopicH(self.url, self.username, self.password, self.token).DeleteTopic(topicname)

    # Processor Handler

    def CreateProcessor(self, name, sql, runners, clusterName, namespace="", pipeline=""):
        """

        :param sql:
        :param runners:
        :param clusterName:
        :param namespace:applies for Kubernetes mode
        :param pipeline:applies for Kubernetes mode
        :return:
        """
        return PrcH(self.url, self.username, self.password, self.token).CreateProcessor(name, sql,
                                                                                        runners, clusterName,
                                                                                        namespace, pipeline)

    def DeleteProcessor(self, processorname):
        """

        :param processorname:
        :return:
        """
        return PrcH(self.url, self.username, self.password, self.token).DeleteProcessor(processorname)

    def ResumeProcessor(self, processorname):
        """

        :param processorname:
        :return:
        """
        return PrcH(self.url, self.username, self.password, self.token).ResumeProcessor(processorname)

    def PauseProcessor(self, processorname):
        """

        :param processorname:
        :return:
        """
        return PrcH(self.url, self.username, self.password, self.token).PauseProcessor(processorname)

    def UpdateProcessorRunners(self, processorName, numberOfRunners):
        """

        :param processorName:
        :param numberOfRunners:
        :return:
        """
        return PrcH(self.url, self.username, self.password, self.token).UpdateProcessorRunners(processorName,
                                                                                               numberOfRunners)
    # Schemas Handler

    def GetAllSubjects(self):
        """

        :return:
        """
        return SchemaH(self.url, self.username, self.password, self.token).ListAllSubjects()

    def ListVersionsSubj(self, subject):
        return SchemaH(self.url, self.username, self.password, self.token).ListVersionsSubj(subject)

    def GetSchemaById(self, subjid):
        return SchemaH(self.url, self.username, self.password, self.token).GetSchemaById(subjid)

    def GetSchemaByVer(self, subject, verid):
        return SchemaH(self.url, self.username, self.password, self.token).GetSchemaByVer(subject, verid)

    def RegisterNewSchema(self, subject, schema_json="", filename=""):
        return SchemaH(self.url, self.username, self.password, self.token).RegisterNewSchema(subject, schema_json, filename)

    def GetGlobalCompatibility(self):
        return SchemaH(self.url, self.username, self.password, self.token).GetGlobalCompatibility()

    def GetCompatibility(self, subject):
        return SchemaH(self.url, self.username, self.password, self.token).GetCompatibility(subject)

    def DeleteSubj(self, subject):
        return SchemaH(self.url, self.username, self.password, self.token).DeleteSubj(subject)

    def DeleteSchemaByVersion(self, subject, version):
        return SchemaH(self.url, self.username, self.password, self.token).DeleteSchemaByVersion(subject, version)

    def ChangeCompatibility(self, subject, compability="", filename=""):
        return SchemaH(self.url, self.username, self.password, self.token).ChangeCompatibility(subject, compability, filename)

    def UpdateGlobalCompatibility(self, compatibility="", filename=""):
        return SchemaH(self.url, self.username, self.password, self.token).UpdateGlobalCompatibility(compatibility, filename)

    # Connector Handler

    def ListAllConnectors(self, cluster):
        return ConnH(self.url, self.username, self.password, self.token).ListAllConnectors(cluster)

    def GetInfoConnector(self, cluster, connector):
        return ConnH(self.url, self.username, self.password, self.token).GetInfoConnector(cluster,connector)

    def GetConnectorConfig(self, cluster, connector):
        return ConnH(self.url, self.username, self.password, self.token).GetConnectorConfig(cluster,connector)

    def GetConnectorStatus(self, cluster, connector):
        return ConnH(self.url, self.username, self.password, self.token).GetConnectorStatus(cluster, connector)

    def GetConnectorTasks(self, cluster, connector):
        return ConnH(self.url, self.username, self.password, self.token).GetConnectorTasks(cluster, connector)

    def GetStatusTask(self, cluster, connector, task_id):
        return ConnH(self.url, self.username, self.password, self.token).GetStatusTask(cluster, connector, task_id)

    def RestartConnectorTask(self, cluster, connector, task_id):
        return ConnH(self.url, self.username, self.password, self.token).RestartConnectorTask(cluster, connector,
                                                                                              task_id)

    def GetConnectorPlugins(self, cluster):
        return ConnH(self.url, self.username, self.password, self.token).GetConnectorPlugins(cluster)

    def PauseConnector(self, cluster, connector):
        return ConnH(self.url, self.username, self.password, self.token).PauseConnector(cluster,connector)

    def ResumeConnector(self, cluster, connector):
        return ConnH(self.url, self.username, self.password, self.token).ResumeConnector(cluster, connector)

    def RestartConnector(self, cluster, connector):
        return ConnH(self.url, self.username, self.password, self.token).RestartConnector(cluster, connector)

    def CreateConnector(self, cluster, config="", filename=""):
        """

        :param cluster:
        :param config:Is a Python dictionary with specific format,see in class ConnectorHandler
        :param filename: In case we want to use file to parse config parameter we create a file with section as we wish
        but option name must be config
        :return:
        """
        return ConnH(self.url, self.username, self.password, self.token).CreateConnector(cluster, config, filename)

    def SetConnectorConfig(self, cluster, connector, config="", filename=""):
        return ConnH(self.url, self.username, self.password, self.token).SetConnectorConfig(cluster, connector, config,
                                                                                            filename)

    def DeleteConnector(self, cluster, connector):
        return ConnH(self.url, self.username, self.password, self.token).DeleteConnector(cluster, connector)

    # WebSocketHandler
    def SubscribeHandler(self, url_ws, clientId, query, write=False, filename="", print_results=True, datetimelist=[],
                         formatinglist=[]):
        """

        :param url_ws:
        :param clientId:
        :param query:
        :param write: If is true create a log file, where values loaded
        :param filename: Where logs save
        :param print_results: If is true print the results
        :param datetimelist: List of keys which content datetime string
        :param formatinglist: List of formation of elements of datetimelist keys
        :return:
        """
        return SubSH(self.username, self.password, clientId, url_ws, query, write, filename, print_results,
                     datetimelist, formatinglist)

    def Publish(self, url_ws, clientId, topic, key, value):
        return PuHandl(self.username, self.password, clientId, url_ws).Publish(topic, key, value)

    def Commit(self, url_ws, clientId, topic, partition, offset):
        return PuHandl(self.username, self.password, clientId, url_ws).Commit(topic, partition, offset)

    def Unscribe(self, url_ws, clientId, topic):
        return PuHandl(self.username, self.password, clientId, url_ws).Unscribe(topic)

    def Subscribe(self, url_ws, clientId, query):
        return PuHandl(self.username, self.password, clientId, url_ws).Subcribe(query)

    # ACL Handler

    def GetACLs(self):
        return ACLHandler(self.token, self.url).GetACL()

    def SetACL(self, resourceType, resourceName, principal, permissionType, host, operation):
        ACLHandler(self.token, self.url).SetAcl(resourceType, resourceName, principal, permissionType, host, operation)

    # Quotas Handler

    def GetQuotas(self):
        return QuotaHandler(self.token, self.url).GetQuotas()

    def SetQuotasAllUsers(self, config):
        QuotaHandler(self.token, self.url).SetQuotasAllUsers(config)

    def SetQuotaUserAllClients(self, user, config):
        QuotaHandler(self.token, self.url).SetQuotaUserAllClients(user, config)

    def SetQuotaUserClient(self, user, clientid, config):
        QuotaHandler(self.token, self.url).SetQuotaUserClient(user, clientid, config)

    def SetQuotaUser(self, user, config):
        QuotaHandler(self.token, self.url).SetQuotaUser(user, config)

    def SetQuotaAllClient(self, config):
        QuotaHandler(self.token, self.url).SetQuotaAllClient(config)

    def SetQuotaClient(self, clientid, config):
        QuotaHandler(self.token, self.url).SetQuotaClient(clientid, config)

    def DeleteQutaAllUsers(self, config):
        QuotaHandler(self.token, self.url).DeleteQutaAllUsers(config)

    def DeleteQuotaUserAllClients(self, user, config):
        QuotaHandler(self.token, self.url).DeleteQuotaUserAllClients(user, config)

    def DeleteQuotaUserClient(self, user, clientid, config):
        QuotaHandler(self.token, self.url).DeleteQuotaUserClient(user, clientid, config)

    def DeleteQuotaUser(self, user, config):
        QuotaHandler(self.token, self.url).DeleteQuotaUser(user, config)

    def DeleteQuotaAllClients(self, config):
        QuotaHandler(self.token, self.url).DeleteQuotaAllClients(config)

    def DeleteQuotaClient(self, clientid, config):
        QuotaHandler(self.token, self.url).DeleteQuotaClient(clientid, config)




