import websocket
import _thread as thread
import json
from pprint import pprint as pp
import time
import ast

from lenses_python.ConvertDateTime import ConvertDateTime

class SubscribeHandler:

    def __init__(self, username, password, clientId, url, query, write, filename, print_results, datetimelist, formatinglist):
        """

        :param username:
        :param password:
        :param clientId:
        :param url:
        :param query:
        :param datetimelist:
        :param formatinglist:
        """
        self.username = username
        self.password = password
        self.clientId = clientId
        self.write = write
        self.filename = filename
        self.print_results = print_results
        self.datetimelist = datetimelist
        self.formatinglist = formatinglist
        self.url = url
        self.data_lst = []  # In this list save all messages which get
        self.query = query
        self._Login()
        self.Subscribe()

    def CreateLogFile(self):
        """
        If self.write is True , write a file with the values of data . It save them as JSON with with jsons

        :return:
        """
        time.sleep(2)
        with open(self.filename, "w+") as f:
            json.dump(self.data_lst, f)


    def on_message(self, ws, message):
        """

        :param ws:
        :param message:
        :return:
        """
        message = json.loads(message)
        if message['type'] == 'KAFKAMSG':
            lst = message['content']
            if self.write:
                if self.filename != "":
                    # self.data_lst = list(map(lambda x: json.loads(x["value"]), lst))
                    self.data_lst = list(map(lambda x: ast.literal_eval(x["value"]), lst))
                    # Check length of lists self.datetimelist and self.formatinglist and if both of them has length
                    # greater than zero then convert keys which contet to self.datetimelist to datetime objects
                    if len(self.datetimelist) > 0 and len(self.formatinglist) > 0:
                        self.data_lst = ConvertDateTime(self.data_lst, self.datetimelist, self.formatinglist)
                    self.CreateLogFile()
                else:
                    raise Exception("Filename of logs is empty..\n")
            if self.print_results:
                # If self.print_results is true print the content of messages, else not print
                for i in lst:
                    pp(i)
        elif message['type'] == 'ERROR':
            raise Exception("Type:{}. Content:{}".format(message["type"], message["content"]))

    def on_error(self, ws, error):
        """

        :param ws:
        :param error:
        :return:
        """
        print(error)

    def on_close(self, ws):
        """

        :param ws:
        :return:
        """
        ws.close()

    def on_open(self, ws):
        """

        :param ws:
        :return:
        """
        request = {  "type" : "SUBSCRIBE",
                   "content" : '{"sqls" : ["'+self.query+'"]}',
                   "correlationId" : 1,
                   "authToken" : self.token
                   }
        def run(*args):
            temp = 0
            while True:
                if temp == 0:
                    ws.send(json.dumps(request))
                    temp = 1
        thread.start_new_thread(run, ())

    def Subscribe(self):
        """
        Make a web socket app , which run forever

        :return:
        """
        ws = websocket.WebSocketApp(self.url_req,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        ws.on_open = self.on_open
        ws.run_forever()


    def _Login(self):
        """
        Make the first connection to  make the login request to take the token,which use

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



