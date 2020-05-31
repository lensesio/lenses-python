from lensesio.core.endpoints import getEndpoints
from threading import Thread, enumerate, RLock
from sys import exc_info
import websocket
import json
import ssl

class DataSubscribe():

    def __init__(self, active_threads, service_account=None, verify_cert=True):
        getEndpoints.__init__(self, "websocketEndpoints")

        self.verify_cert=verify_cert
        self.lenses_websocket_endpoint = self.url + self.lensesWebsocketEndpoint

        self.service_account = service_account
        if self.service_account:
            self.wc_conn_token = self.service_account

        if 'https' in self.lenses_websocket_endpoint:
            self.lenses_websocket_endpoint = self.lenses_websocket_endpoint.replace(
                "https", "wss"
            )
        else:
            self.lenses_websocket_endpoint = self.lenses_websocket_endpoint.replace(
                "http", "ws"
            )

        self.sub_active_threads = active_threads

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

        if self.verify_cert:
            ws = websocket.create_connection(
                self.url_req
            )
        else:
            ws = websocket.create_connection(
                self.url_req,
                sslopt={"cert_reqs": ssl.CERT_NONE}
            )

        ws.send(json.dumps(loginrequest))

        response = json.loads(ws.recv())
        self.wc_conn_token = response['content']

    def Publish(self, topic, key, value, clientId=1):
        if self.service_account:
            self.wc_conn_token == self.service_account
            self.url_req = self.lenses_websocket_endpoint + str(clientId)
        else:
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
            if self.verify_cert:
                ws_con = websocket.create_connection(
                    self.url_req
                )
            else:
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
            if self.verify_cert:
                ws_con = websocket.create_connection(
                    self.url_req
                )
            else:
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

    def stop_subscribe_query(self, t, stop=False):
        sqlthread = self.sub_active_threads['subscribe'][t]['thread']
        if not sqlthread.is_alive():
            self.sub_active_threads['subscribe'][t]['state'] = False
            return self.subscribe_state_report(t)

        if stop and self.sub_active_threads['subscribe'][t]['state']:
            print("Subscribe Thread has been marked for shutdown")
            self.sub_active_threads['subscribe'][t]['state'] = False
            self.running_state = False

        return self.subscribe_state_report(t)

    def subscribe_state_report(self, t):
        sqlthread = self.sub_active_threads['subscribe'][t]['thread']
        if self.sub_active_threads['subscribe'][t]['state'] and sqlthread.is_alive():
            msg = "Subscribe Thread is running"
        elif not self.sub_active_threads['subscribe'][t]['state'] and sqlthread.is_alive():
            msg = "Waiting for thread to shutdown"
        else:
            msg = "Subscribe Thread has closed"

        return msg, self.sub_active_threads['subscribe'][t]['state']

    def Subscribe(self, dataFunc, query, clientId=1, spawn_thread=None):
        if spawn_thread:
            self.sub_active_threads['thread_lock'].acquire()
            self.sub_active_threads['subscribe']['t'] += 1
            t = self.sub_active_threads['subscribe']['t']
            self.sub_active_threads['thread_lock'].release()

            self.new_sql = Thread(
                target=self.exec_subscribe,
                args=(
                    dataFunc,
                    query,
                    clientId,
                    t,
                ),
                daemon=False
            )

            self.sub_active_threads['thread_lock'].acquire()
            self.sub_active_threads['subscribe'][t] = {
                'dataFunc': dataFunc,
                'query': query,
                'clientId': clientId,
                "state": False,
                "state_info": None,
                "thread": self.new_sql,
                "topic_metadata": {}
            }

            self.new_sql.start()
            self.sub_active_threads['thread_lock'].release()

            print(
                "SQL Thread -\t with SUBSCRIBE_ID: %s has been started\n" % t,
                "You may find thread info at callers_name.active_threads object\n"
            )
        elif not spawn_thread:
            self.execSQL = self.exec_subscribe(dataFunc, query, clientId, t=None)
            return self.execSQL

    def exec_subscribe(self, dataFunc, query, clientId=1, t=None):
        def update_thread_state(msg, state=True):
            if t:
                self.sub_active_threads['thread_lock'].acquire()
                self.sub_active_threads['subscribe'][t]['state'] = state
                self.sub_active_threads['subscribe'][t]['state_info'] = msg

                if not state:
                    self.running_state = False

                self.sub_active_threads['thread_lock'].release()

        def update_subscribe_metadata(message):
            if t:
                topic = message['topic']
                partition = message['partition']
                offset = int(message['offset'])

                self.sub_active_threads['thread_lock'].acquire()
                if self.sub_active_threads['subscribe'][t]['topic_metadata'].get(topic):
                    key_exists = self.sub_active_threads['subscribe'][t][
                        'topic_metadata'
                    ][topic]["partitions"].get(partition)
                    if key_exists:
                        self.sub_active_threads['subscribe'][t][
                            'topic_metadata'
                        ][topic]["partitions"][partition]["offset"] = offset
                    else:
                        self.sub_active_threads['subscribe'][t][
                            'topic_metadata'
                        ][topic]["partitions"][partition] = {
                            "offset": offset
                        }

                    self.sub_active_threads['subscribe'][t]['topic_metadata'][topic]["total_records"] += 1

                else:
                    self.sub_active_threads['subscribe'][t]['topic_metadata'][topic] = {
                        "partitions": {
                            partition: {
                                "offset": offset
                            }
                        },
                        "total_records": 1
                    }

                self.sub_active_threads['thread_lock'].release()

        update_thread_state("Started")
        self.running_state = True
        if self.service_account:
            self.wc_conn_token == self.service_account
            self.url_req = self.lenses_websocket_endpoint + str(clientId)
        else:
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

        if self.verify_cert:
            ws_con = websocket.create_connection(
                self.url_req
            )
        else:
            ws_con = websocket.create_connection(
                self.url_req,
                sslopt={"cert_reqs": ssl.CERT_NONE}
            )

        update_thread_state("Created websocket client")
        ws_con.send(json.dumps(request))
        update_thread_state("Executing query")

        try:
            while self.running_state:
                bucket = json.loads(ws_con.recv())

                if bucket['type'] == 'KAFKAMSG':
                    for message in bucket['content']:
                        dataFunc(message)
                        update_subscribe_metadata(message)
                    # # Auto commit is disabled for now. We need to switch the websocket request
                    # # to successfully list the client as a subscriber in order for auto-commit to work
                    # # Part of ToDo 
                    # self.Commit(
                    #     payload=bucket,
                    #     token=self.wc_conn_token,
                    #     clientId=bucket['correlationId']
                    # )
                elif bucket['type'] in ['HEARTBEAT', 'SUCCESS']:
                    dataFunc(bucket)
                elif bucket['type'] in ['ERROR']:
                    update_thread_state(bucket['content'], False)

            ws_con.close()
            update_thread_state("Subscribe query execution finished successfully", False)
        except KeyboardInterrupt:
            update_thread_state("")
            ws_con.close()
        except:
            ws_con.close()
            update_thread_state(str(exc_info()), False)
            raise

    def Unsubscribe(self, topic, clientId=1):
        if self.service_account:
            self.wc_conn_token == self.service_account
            self.url_req = self.lenses_websocket_endpoint + str(clientId)
        else:
            self._Login(clientId)

        requestjson = {
            "type": "UNSUBSCRIBE",
            "content": '{"topics": ["' + topic + '"]}',
            "correlationId": int(clientId),
            "authToken": self.wc_conn_token,
        }

        try:
            if self.verify_cert:
                ws_con = websocket.create_connection(
                    self.url_req
                )
            else:
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
