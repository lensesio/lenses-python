from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request


class KafkaTopic():

    def __init__(self, verify_cert=True):
        getEndpoints.__init__(self, "topicEndpoints")

        self.verify_cert=verify_cert
        self.topic_end_point = self.url + self.lensesTopicsEndpoint
        self.topic_config_end_point = self.url + self.lensesTopicsConfigEndpoint
        self.manage_topic_endpoint = self.url + self.lensesManageTopicEndpoint
        self.topic_config_update = self.url + self.lensesTopicConfigUpdate
        self.topic_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/plain',
            'x-kafka-lenses-token': self.token
        }

    def GetAllTopics(self):
        self.allTopics = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=self.topic_end_point,
            __HEADERS=self.topic_headers,
            __VERIFY=self.verify_cert
        )

        return self.allTopics

    def LstOfTopicsNames(self):
        self.GetAllTopics()
        self.listOfTopicNames = [i['topicName'] for i in self.allTopics]
        return self.listOfTopicNames

    def TopicInfo(self, topicname):
        __RQE = self.manage_topic_endpoint + '/' + topicname
        self.topicInfo = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.topic_headers,
            __VERIFY=self.verify_cert
        )

        return self.topicInfo

    def UpdateTopicConfig(self, topicname, data_params):
        """

        :param topicname:
        :param data_params: Must be in dict format, for example,
        {
            "configs": [{
                        "key": "cleanup.policy",
                        "value": "compact"
                        }]
        }
        """
        __RQE = self.topic_config_update + '/' + topicname
        self.updateTopicConfig = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.topic_headers,
            __DATA=data_params,
            __VERIFY=self.verify_cert
        )

        return self.updateTopicConfig

    def CreateTopic(self, topicName, replication, partitions, config):
        """
        Example of request
        {
            "topicName": "topicA",
            "replication": 1,
            "partitions": 1,
            "configs": {
            "cleanup.policy": "compact",
            "compression.type": "snappy"
                        }
        }
        :param topicName:Name of topic
        :param replication:
        :param partitions:
        :param config: is dict ,for example
        {
            "cleanup.policy": "compact",
            "compression.type": "snappy"
        }
        """
        params = dict(
            topicName=topicName,
            replication=int(replication),
            partitions=int(partitions),
            configs=config
        )

        self.createTopic = exec_request(
            __METHOD="post",
            __EXPECTED="text",
            __URL=self.manage_topic_endpoint,
            __HEADERS=self.topic_headers,
            __DATA=params,
            __VERIFY=self.verify_cert
        )

        return self.createTopic

    def DeleteTopic(self, topicname):
        """

        :param topicname:
        """
        __RQE = self.manage_topic_endpoint
        __RQE = __RQE + '/' + topicname
        self.deleteTopic = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.topic_headers,
            __VERIFY=self.verify_cert
        )

        return self.deleteTopic

    def DeleteTopicRecords(self, topic, partition, offset):
        """

        :param topic:
        :param partition:
        :param offset:
        """
        __RQE = self.manage_topic_endpoint
        __RQE = __RQE + '/' + topic
        __RQE = __RQE + '/' + partition
        __RQE = __RQE + '/' + offset
        self.deleteTopicRecords = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.topic_headers,
            __VERIFY=self.verify_cert
        )

        return self.deleteTopicRecords

    def DefaultTopicConfigs(self):
        self.defaultTopicConfigs = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=self.topic_config_end_point,
            __HEADERS=self.topic_headers,
            __VERIFY=self.verify_cert
        )

        return self.defaultTopicConfigs

    def AvailableTopicConfigKeys(self):
        __RQE = self.topic_config_end_point + "/keys"
        self.availableTopicConfigKeys = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.topic_headers,
            __VERIFY=self.verify_cert
        )

        return self.availableTopicConfigKeys
