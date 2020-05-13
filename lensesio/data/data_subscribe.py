from lensesio.core.endpoints import getEndpoints
import websocket
import json
import ssl

class DataSubscribe():

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

    def _Login(self, clientId):
        if self.auth_type == 'krb5':
            self.wc_conn_token = self.token
            self.url_req = self.lenses_websocket_endpoint + str(clientId)
            return self.token

        auth_payload = {
            "user": self.username,
            "password": self.password
        }
        loginrequest = {
            "type": "LOGIN",
            "content": json.dumps(auth_payload),
            "correlationId": int(clientId),
            "authToken": ""
        }

        self.url_req = self.lenses_websocket_endpoint + str(clientId)

        ws = websocket.create_connection(
            self.url_req,
            sslopt={"cert_reqs": ssl.CERT_NONE}
        )
        ws.send(json.dumps(loginrequest))

        response = json.loads(ws.recv())
        self.wc_conn_token = response['content']

    def Publish(self, topic, key, value, clientId=1):
        self._Login(clientId)

        publish_payload = {
            "topic" : topic,
            "key": key,
            "value": value
        }

        requestdict = {
            "type": "PUBLISH",
            "content": json.dumps(publish_payload),
            "correlationId": int(clientId),
            "authToken": self.wc_conn_token
        }

        try:
            ws_con = websocket.create_connection(
                self.url_req,
                sslopt={"cert_reqs": ssl.CERT_NONE}
            )
            ws_con.send(json.dumps(requestdict))

            self.publish = json.loads(ws_con.recv())
            ws_con.close()
        except KeyboardInterrupt:
            ws_con.close()
            return None

        return self.publish

    def GetCommits(self, payload):
        def check_if_message(message):
            is_msg = False
            for e in ['topic', 'partition', 'offset']:
                if e not in message.keys():
                    break
            else:
                is_msg = True
            
            return is_msg

        if type(payload) is not dict:
            return None

        commits = []
        if payload.get('content'):
            for msg in payload['content']:
                commits.append(
                    {
                        "topic": msg['topic'],
                        "partition": msg['partition'],
                        "offset" : int(msg['offset'])
                    }
                )
        elif check_if_message(payload):
            commits = [
                {
                    "topic": payload['topic'],
                    "partition": payload['partition'],
                    "offset" : int(payload['offset'])
                }
            ]
        else:
            return None

        return commits

    def Commit(self, payload, token, clientId=1):
        commits = self.GetCommits(payload)
        if not commits:
            return "Invalid payload: %s. No commit was made" % (
                payload
            )

        requestdict = {
            "type": "COMMIT",
            "content": json.dumps({"commits": commits}),
            "correlationId": int(clientId),
            "authToken": token
        }

        try:
            ws_con = websocket.create_connection(
                self.url_req,
                sslopt={"cert_reqs": ssl.CERT_NONE}
            )
            ws_con.send(json.dumps(requestdict))

            self.commit = json.loads(ws_con.recv())
            ws_con.close()
        except KeyboardInterrupt:
            ws_con.close()
            return None

        return self.commit

    def Subscribe(self, dataFunc, query, clientId=1):
        self._Login(clientId)

        if type(query) is list: 
            sql_query = {
                "sqls": query
            }
        elif type(query) is str:
            sql_query = {
                "sqls": [query]
            }
        else:
            return "Error. Please provide either a query (type str) or a list of queries [q1, ...]"

        request = {
            "type": "SUBSCRIBE",
            "content": json.dumps(sql_query),
            "correlationId": int(clientId),
            "authToken": self.wc_conn_token
        }

        ws_con = websocket.create_connection(
            self.url_req,
            sslopt={"cert_reqs": ssl.CERT_NONE}
        )
        ws_con.send(json.dumps(request))
        try:
            while True:
                bucket = json.loads(ws_con.recv())

                if bucket['type'] == 'KAFKAMSG':
                    for message in bucket['content']:
                        dataFunc(message)
## Auto commit is disabled for now. We need to switch the websocket request
## to successfully list the client as a subscriber in order for auto-commit to work
## Part of ToDo 
#                     self.Commit(
#                         payload=bucket,
#                         token=self.wc_conn_token,
#                         clientId=bucket['correlationId']
#                     )
                elif bucket['type'] in ['HEARTBEAT', 'SUCCESS']:
                    dataFunc(bucket)

            ws_con.close()
        except KeyboardInterrupt:
            ws_con.close()

    def Unsubscribe(self, topic, clientId=1):
        self._Login(clientId)

        requestjson = {
            "type": "UNSUBSCRIBE",
            "content": '{"topics": ["' + topic + '"]}',
            "correlationId": int(clientId),
            "authToken": self.wc_conn_token,
        }

        try:
            ws_con = websocket.create_connection(
                self.url_req,
                sslopt={"cert_reqs": ssl.CERT_NONE}
            )
            ws_con.send(json.dumps(requestjson))

            self.unscribe = json.loads(ws_con.recv())
            ws_con.close()
        except KeyboardInterrupt:
            ws_con.close()
            return None
        
        return self.unscribe
