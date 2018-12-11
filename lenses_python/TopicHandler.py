from requests import get, delete, post, put
from pprint import pprint as pp
from lenses_python.ReadConfigFile import ReadConfigFile
from lenses_python.constants import TOPIC_CONFIG_END_POINT, TOPIC_END_POINT

class TopicHandler:

    def __init__(self, url, username, password, token):
        self.url = url
        self.username = username
        self.password = password
        self.token = token
        self.topic_end_point = TOPIC_END_POINT
        self.topic_config_end_point = TOPIC_CONFIG_END_POINT
        self.default_headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                                'x-kafka-lenses-token': self.token}

    def GetAllTopics(self):
        # url = self.url+"/api/topics"
        url = self.url+self.topic_end_point
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def LstOfTopicsNames(self):
        """

        :return:
        """
        alltopics = self.GetAllTopics()
        lstoftopicsnames = [i['topicName'] for i in alltopics]
        return lstoftopicsnames

    def TopicInfo(self, topicname):
        """

        :param topicname:
        :return:
        """
        url = self.url+self.topic_end_point
        response = get(url+"/"+topicname, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        else:
            return response.json()

    def UpdateTopicConfig(self, topicname, data_params, filename):
        """

        :param topicname:
        :param data_params: Must be in dict format, for example, {
                                                                   "configs": [{
                                                                               "key": "cleanup.policy",
                                                                                "value": "compact"
                                                                              }]
                                                                 }
        :param filename:
        :return:
        """
        if topicname == "" and data_params == "" and filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("config", None) is not None and temp_dict.get("topicname", None) is not None:
                data_params = temp_dict["config"]
                topicname = temp_dict["topicname"]
            else:
                raise Exception("In file there aren't options config and topicname\n")
        elif filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("config", None) is not None:
                # The option name of option must be config and have json format
                data_params = temp_dict["config"]
            else:
                raise Exception("In file there isn't option config\n")
        headers = {'Content-Type': 'application/json', 'Accept': 'text/plain',
                   'x-kafka-lenses-token': self.token}
        url = self.url+"/api/configs/topics/"+topicname
        response = put(url, headers=headers, json=data_params)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))

    def CreateTopic(self, topicName, replication, partitions, config, filename):
        """
        Example of request {
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
        :param config: is dict ,for example {
        "cleanup.policy": "compact",
        "compression.type": "snappy"
         }
        :param filaname:
        :return:
        """
        if topicName == "" and replication == "" and partitions == "" and config == "" and filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("topicname", None) is not None and temp_dict.get("replication", None) is not None and \
                            temp_dict.get("partitions", None) is not None and temp_dict.get("config", None) is not None:
                # The options name must be topicName,replication,partitions and config
                topicName = temp_dict["topicname"]
                replication = temp_dict["replication"]
                partitions = temp_dict["partitions"]
                config = temp_dict["config"]
            else:
                raise Exception("In file there aren't the sections topicName,replication,partitions and config\n")
        elif filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("config", None) is not None:
                # The option name of option must be config and have json format
                config = temp_dict["config"]
            else:
                raise Exception("Failed to read configuaration file\n")
        headers = {'Content-Type': 'application/json', 'Accept': 'text/plain',
                   'x-kafka-lenses-token': self.token}
        url = self.url+self.topic_end_point
        params = dict(topicName=topicName,
                      replication=int(replication),
                      partitions=int(partitions),
                      configs=config
                      )
        response = post(url, headers=headers, json=params)
        if response.status_code != 201:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))

    def DeleteTopic(self, topicname):
        headers = {'Content-Type': 'application/json', 'Accept': 'text/plain',
                   'x-kafka-lenses-token': self.token}

        url = self.url+self.topic_end_point
        response = delete(url+"/"+topicname, headers=headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))

    def DeleteTopicRecords(self, topic, partition, offset):
        """
        New endpoint for 2.1
        The endpoint is the /api/topics/{topicName}/{partition}/{offset}

        :param topic:
        :param partition:
        :param offset:
        :return:
        """
        headers = {'Content-Type': 'application/json', 'Accept': 'text/plain',
               'x-kafka-lenses-token': self.token}
        url = self.url+self.topic_end_point
        response = delete(url+"/"+topic+"/"+partition+"/"+offset, headers=headers)
        if response.status_code != 200:
            if response.status_code == 400:
                raise Exception("Http status code {} The offset {} is negative".format(response.status_code, offset))
            elif response.status_code == 404:
                raise Exception("Http status code {}. The topic {} doesn't exist".format(response.status_code, topic))
            elif response.status_code == 403:
                raise Exception("Http status code {}. The user doesn't have the"
                            " permission for this action ".format(response.status_code))
            else:
                raise Exception("Http status code {}. {}".format(response.status_code, response.text))
        else:
            return response.text

    def DefaultConfigs(self):
        """
        GET api/configs/default/topics
        :return:
        """
        url = self.url+self.topic_config_end_point
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        pp(response.json())

    def AvailableConfigKeys(self):
        """
        GET /api/configs/default/topics/keys

        :return:
        """
        url = self.url+self.topic_config_end_point+"/keys"
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        pp(response.json())
