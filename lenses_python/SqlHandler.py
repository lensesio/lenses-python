from requests import *
import pandas as pd
from json import loads
from urllib.parse import urlencode
import websocket


from lenses_python.ConvertDateTime import ConvertDateTime

class SqlHandler:

    def __init__(self, url, username, password, token, query="", datetimelist=[], formatinglist=[]):
        """

        :param url:
        :param username:
        :param password:
        :param token:
        :param query:
        :param datetimelist:
        :param formatinglist:
        """
        self.url = url
        self.username = username
        self.password = password
        self.token = token
        self.query = query
        self.datetimelist = datetimelist
        self.formatinglist = formatinglist
        self.params = {'sql': self.query}
        self.default_headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                                'x-kafka-lenses-token': self.token}
        self.default_headers_2 = {'Content-Type': 'text/event-stream', 'Accept': 'text/event-stream',
                                'x-kafka-lenses-token': self.token}
        self._ValidateSqlQuery()

    def _ValidateSqlQuery(self):
        """

        :return:
        """
        VALIDATE_SQL_QUERY='/api/sql/validation'

        url = self.url+VALIDATE_SQL_QUERY
        response = get(url, params=self.params, headers=self.default_headers)
        if response.status_code != 200:
                 raise Exception('An error occurred while trying to validate sql query. Received response with '
                                        '\status code [{}] and text [{}]'.format(response.status_code, response.text))

    def _ConvertToDF(self, data):
        """
        Get data from sql handler and extract from generate dict the messages and then the dict-value from each one

        :param data: list of dictionaries
        :return: pandas dataframe
        """
        if type(data[0]["value"]) != type(dict()):
            data = list(map(lambda x: loads(x["value"]), data))
        else:
            data = list(map(lambda x: x["value"], data))
        if len(self.datetimelist) > 0 and len(self.formatinglist) > 0:
            # If these two lists has length greater than zero , then call class ConvertDateTime which
            # which convert specific keys ,which have datetime string to datetime object
            # this convert can be only if user request data as pandas dataframe
            data = ConvertDateTime(data, self.datetimelist, self.formatinglist).Convert()
        # Convert data to pandas dataframe
        data = pd.DataFrame(data)
        return data

    def browsing_data(self, is_extract_pandas=False):
        """

        :param is_extract_pandas: Boolean
        :return: In case is_extract if false return a dictionary , otherwise return Pandas dataframe
        """
        params = {
                   "token": self.token,
                   "sql": self.query,
                   "stats": 2
                 }
        params = urlencode(params)
        url = self.url.replace("https", "wss")+"/api/sql/execute?"+params
        ws = websocket.create_connection(url)
        data_list = []
        stats_list =[]
        temp_type = ""
        while temp_type.upper != "END":
            temp_data = loads(ws.recv())  # Convert string to dict
            temp_type = temp_data["type"]
            if temp_type.upper == "RECORD":
                data_list.append(temp_data["data"])
            elif temp_type.upper == "STATS":
                stats_list.append(temp_data["data"])
        ws.close()
        if is_extract_pandas:
            return {"records": data_list,
                    "stats": stats_list
                   }
        else:
            if len(data_list) > 0:
                return self._ConvertToDF(data_list)
            else:
                raise Exception("Empty data list")
