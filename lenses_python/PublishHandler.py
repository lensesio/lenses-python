import websocket
import json
from pprint import pprint as pp

class PublishHandler:

    def __init__(self, username, password, clientId, url):
        self.username = username
        self.password = password
        self.url = url
        self.clientId = clientId
        self._Login()

    def _Login(self):
        """

        :return: The token
        """
        loginrequest = {"type": "LOGIN", "content": '{"user": "'+self.username+'", "password": "'+self.password+'"}',
                        "correlationId": 2, "authToken": ""}
        self.url_req = self.url+"/api/kafka/ws/"+self.clientId
        ws = websocket.create_connection(self.url_req)
        ws.send(json.dumps(loginrequest))
        response = json.loads(ws.recv())
        if response['type'] == "ERROR":
            raise Exception("Type:{}. Content:{}".format(response["type"], response["content"]))
        self.token = response['content']

    def Publish(self, topic, key, value):
        requestdict = {"type": "PUBLISH",
                       "content": '{"topic" :"'+topic+'","key" :"'+key+'","value" : "'+value+'"}',
                       "correlationId": 1,
                       "authToken": self.token
                       }
        ws = websocket.create_connection(self.url_req)
        ws.send(json.dumps(requestdict))
        response = json.loads(ws.recv())
        if response['type'] == 'ERROR':
            raise Exception("Type:{}. Content:{}".format(response["type"], response["content"]))
        pp(response)

    def Commit(self, topic, partition, offset):
        """

        :param topic:
        :param partition:
        :param offset:
        :return:
        """
        requestdict = {
            "type": "COMMIT",
            "content": '{"commits": [{"topic":"'+topic+'","partition":"'+partition+'","offset" : "'+offset+'"}]}',
            "correlationId": 1,
            "authToken": self.token
                        }
        ws = websocket.create_connection(self.url_req)
        ws.send(json.dumps(requestdict))
        response = json.loads(ws.recv())
        if response['type'] == 'ERROR':
            raise Exception("Type:{}. Content:{}".format(response["type"], response["content"]))
        pp(response)

    def Subcribe(self, query):
        """

        :param query:
        :return:
        """
        request = {  "type": "SUBSCRIBE",
                     "content": '{"sqls" : ["'+query+'"]}',
                     "correlationId": 1,
                     "authToken": self.token
                     }
        ws = websocket.create_connection(self.url_req)
        ws.send(json.dumps(request))
        response = json.loads(ws.recv())
        if response['type'] == 'ERROR':
            raise Exception("Type:{}. Content:{}".format(response["type"], response["content"]))
        pp(response)

    def Unscribe(self, topic):
        """

        :param topic:
        :return:
        """
        requestjson = {
            "type" :"UNSUBSCRIBE",
            "content": '{"topics": ["'+topic+'"]}',
            "correlationId": 1,
            "authToken": self.token,
        }
        ws = websocket.create_connection(self.url_req)
        ws.send(json.dumps(requestjson))
        response = json.loads(ws.recv())
        if response['type'] in ["ERROR", "INVALIDREQUEST"]:
            raise Exception("Type:{}. Content:{}".format(response["type"], response["content"]))
        pp(response)