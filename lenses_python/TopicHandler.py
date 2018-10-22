from requests import get, post, delete
from json import dumps
from pprint import pprint as pp
from lenses_python.ReadConfigFile import ReadConfigFile

class TopicHandler:

    def __init__(self, parent):
        self.url = parent.url
        self.token = parent.token
        self.default_headers = parent.default_headers

    def GetAllTopics(self, **kopts):
        response = get(self.url + "/api/topics", headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def LstOfTopicsNames(self, **kopts):
        topics = self.GetAllTopics()
        listoftopics = [topic['topicName'] for topic in topics]
        return listoftopics

    def TopicInfo(self, **kopts):
        """
        :param topicname:
        """
        response = get(self.url + "/api/topics" + "/" + kopts['topicname'], headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        else:
            return response.json()

    def UpdateTopicConfig(self, **kopts):
        """
        :param topicname:
        :param data_params: Must be in dict format, for example,
        {
           "configs": [{
                       "key": "cleanup.policy",
                        "value": "compact"
                      }]
         }
        :param filename:
        """
        topicname, data_params, filename = kopts['topicname'], kopts['config'], kopts['filename']
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

        response = put(self.url + "/api/topics/config/" + topicname, headers=headers, json=data_params)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))

    def CreateTopic(self, **kopts):
        """
        Example of request: {
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
        :param config: is dict ,for example: {
            "cleanup.policy": "compact",
            "compression.type": "snappy"
            }
        :param filaname:
        :return:
        """

        topicName, config, filename = kopts['topicname'], kopts['config'], kopts['filename']
        par_rep = {x:kopts[x] if kopts[x] is not '' else 1 for x in ['partitions', 'replication']}

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

        headers = {'Content-Type': 'application/json', 'Accept': '*/*',
                   'x-kafka-lenses-token': self.token}

        params = dict(topicName=topicName,
                      replication=int(par_rep['replication']),
                      partitions=int(par_rep['partitions']),
                      configs=config
                      )

        response = post(self.url + "/api/topics", headers=headers, json=dumps(params))

        if response.status_code != 201:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))

    def DeleteTopic(self, **kopts):
        headers = {'Content-Type': 'application/json', 'Accept': 'text/plain',
                   'x-kafka-lenses-token': self.token}

        response = delete(self.url + "/api/topics" + "/" + kopts['topicname'], headers=headers)

        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))

    def DeleteTopicRecords(self, **kopts):
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

        topic, partition, offset = kopts['topicname'], kopts['partitions'], kopts['offset']

        response = delete(self.url + "/api/topics" + "/" + topic + "/" + partition + "/" + offset, headers=headers)
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
