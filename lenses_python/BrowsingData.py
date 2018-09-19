import websocket
import json
from urllib.parse import urlencode


class BrowsingData:

    def __init__(self, token, url, query):
        """

        :param username:
        :param password:
        :param token:
        :param url:
        :return:
        """
        self.token = token
        self.url = url+"/api/sql/execute?"
        self.query = query
        self.params ={
                       "sql":self.query,
                       "stats":2,
                       "token": self.token
                     }
        self.params = urlencode(self.params)

    def browsing_data(self):
        """

        :return:
        """
        request = self.url+self.params
        ws = websocket.create_connection(request)
        data_list = []
        temp_type = ""
        while temp_type != "END":
            temp_data = json.loads(ws.recv()) # Convert string to dict
            temp_type = temp_data["type"]
            if temp_type == "RECORD":
                data_list.append(temp_data)
        print(data_list)
