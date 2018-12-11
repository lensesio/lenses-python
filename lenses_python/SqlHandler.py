from requests import get, delete, post, put
import pandas as pd
from json import loads
from urllib.parse import urlencode
import websocket

from lenses_python.ConvertDateTime import ConvertDateTime
from lenses_python.constants import VALIDATE_SQL_QUERY, SQL_END_POINT

class SqlHandler:

    def __init__(self, url, username, password, token, query=None, datetimelist=[], formatinglist=[]):
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
        if not isinstance(data[0]["value"], dict):  # Check if is dict , if not do
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

    def browsing_data(self, stats=0,  is_extract_pandas=False):
        """

        :param stats: int
        :param is_extract_pandas: Boolean
        :return: In case is_extract if false return a dictionary , otherwise return Pandas dataframe
        """
        params = {
                   "token": self.token,
                   "sql": self.query,
                   "stats": stats
                 }
        params = urlencode(params)
        if 'https' in self.url:
            url = self.url.replace("https", "wss")+SQL_END_POINT+params
        else:
            url = self.url.replace("http", "ws") + SQL_END_POINT + params
        ws = websocket.create_connection(url)
        data_list = []
        stats_list = []
        while True:
            temp_data = loads(ws.recv())  # Convert string to dict
            temp_type = temp_data.get("type", None)  # If type key doesnt exitst get value None
            if temp_type is None:
                raise KeyError("There isn't key 'type'")  # If there isn't key type in dict raise keyerror exception
            if temp_type == "RECORD":
                data_list.append(temp_data["data"])
            elif temp_type == "STATS":
                stats_list.append(temp_data["data"])
            elif temp_type == "END":
                break  # Exit from while loop
        ws.close()
        if not is_extract_pandas:
            return {"records": data_list,
                    "stats": stats_list
                   }
        else:
            if len(data_list) > 0:
                return self._ConvertToDF(data_list)
            else:
                raise Exception("Empty data list")
