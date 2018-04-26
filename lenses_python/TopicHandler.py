from requests import *
from pprint import pprint as pp
from lenses_python.ReadConfigFile import ReadConfigFile

class TopicHandler:

    def __init__(self, url, username, password, token):
        self.url = url
        self.username = username
        self.password = password
        self.token = token
        self.default_headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                                'x-kafka-lenses-token': self.token}

    def GetAllTopics(self):
        url = self.url+"/api/topics"
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
        url = self.url+"/api/topics"
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

        url = self.url+"/api/topics/config/"+topicname
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
        url = self.url+"/api/topics"
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

        url = self.url+"/api/topics"
        response = delete(url+"/"+topicname, headers=headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))



