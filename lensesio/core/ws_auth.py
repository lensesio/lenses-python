import websocket
import json


class WebsocketAuth():
    def _Login(self, clientId):
        if self.auth_type == 'krb5':
            self.wc_conn_token = self.token
            self.url_req = self.lenses_websocket_endpoint + clientId
            return self.token

        tmp_content = '{"user": "' + self.username + '", "password": "' + self.password + '"}'
        loginrequest = {
                        "type": "LOGIN",
                        "content": tmp_content,
                        "correlationId": 2,
                        "authToken": ""
        }

        self.url_req = self.lenses_websocket_endpoint + clientId

        ws = websocket.create_connection(self.url_req)
        ws.send(json.dumps(loginrequest))

        response = json.loads(ws.recv())
        self.wc_conn_token = response['content']
