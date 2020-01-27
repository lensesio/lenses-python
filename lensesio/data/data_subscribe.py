from lensesio.core.ws_auth import WebsocketAuth
from lensesio.core.endpoints import getEndpoints
import websocket
import json


class DataSubscribe(WebsocketAuth):

    def __init__(self):
        getEndpoints.__init__(self, "websocketEndpoints")

        self.lenses_websocket_endpoint = self.url + self.lensesWebsocketEndpoint

        if 'https' in self.lenses_websocket_endpoint:
            self.lenses_websocket_endpoint = self.lenses_websocket_endpoint.replace(
                "https", "wss"
            )
        else:
            self.lenses_websocket_endpoint = self.lenses_websocket_endpoint.replace(
                "http", "ws"
            )

    def Publish(self, topic, key, value, clientId='LensesPy'):
        self._Login(clientId)

        requestdict = {
            "type": "PUBLISH",
            "content": '{"topic" :"' + topic + '","key" :"' + key + '","value" : "' + value + '"}',
            "correlationId": 1,
            "authToken": self.wc_conn_token
        }

        ws_con = websocket.create_connection(self.url_req)
        ws_con.send(json.dumps(requestdict))

        self.publish = json.loads(ws_con.recv())
        ws_con.close()
        return self.publish

    def Commit(self, topic, partition, offset, clientId='LensesPy'):
        self._Login(clientId)

        requestdict = {
            "type": "COMMIT",
            "content": '{"commits": [{"topic":"' + str(topic) + '","partition":"' + str(partition) + '","offset" : "' + str(offset) + '"}]}',
            "correlationId": 1,
            "authToken": self.wc_conn_token
            }

        ws_con = websocket.create_connection(self.url_req)
        ws_con.send(json.dumps(requestdict))

        self.commit = json.loads(ws_con.recv())
        ws_con.close()
        return self.commit

    def Subscribe(self, dataFunc, query, clientId='LensesPy'):
        self._Login(clientId)

        request = {
            "type": "SUBSCRIBE",
            "content": '{"sqls" : ["' + query + '"]}',
            "correlationId": 1,
            "authToken": self.wc_conn_token
        }

        ws_con = websocket.create_connection(self.url_req)
        ws_con.send(json.dumps(request))

        while True:
            bucket = json.loads(ws_con.recv())
            if bucket['type'] == 'KAFKAMSG':
                for message in bucket['content']:
                    dataFunc(message)
            elif bucket['type'] in ['HEARTBEAT', 'SUCCESS']:
                dataFunc(bucket)

        ws_con.close()

    def Unsubscribe(self, topic, clientId='LensesPy'):
        self._Login(clientId)

        requestjson = {
            "type": "UNSUBSCRIBE",
            "content": '{"topics": ["' + topic + '"]}',
            "correlationId": 1,
            "authToken": self.wc_conn_token,
        }

        ws_con = websocket.create_connection(self.url_req)
        ws_con.send(json.dumps(requestjson))

        self.unscribe = json.loads(ws_con.recv())
        ws_con.close()
        return self.unscribe
