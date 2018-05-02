from requests import *
import pandas as pd
from json import loads
import sseclient

from lenses_python.ConvertDateTime import ConvertDateTime

class SqlHandler:

    def __init__(self, url, username, password, token, query, datetimelist, formatinglist):
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



    def _with_requests(self, url):
        """
        Use GET request to get the data
        :param url:
        :return:
        """
        return get(url, params=self.params, headers=self.default_headers_2, stream=True)

    def ExecuteSqlQuery(self, extract_pandas):
        """
        For SSE protocol use https://pypi.python.org/pypi/sseclient-py
        ticket LEN-134
        :param extract_pandas:
        :return:if extract_pandas is empty return a list of dictionaries , otherwise return a pandas dataframe
        """

        EXECUTE_SQL_QUERY = "/api/sql/data"
        url = self.url+EXECUTE_SQL_QUERY
        response = self._with_requests(url)
        client = sseclient.SSEClient(response)
        messages = []
        offset = []
        data = {}
        for event in client.events():
            msg = event.data
            if msg[0] == "0":
                # Empty messages are just heartbeat
                pass
            else:
                if msg[0] == "2":
                    # 2 is offset details
                    break
                    # offset.append(msg[1:])
                elif msg[0] == "3":
                    # 3 is an error entry
                    raise Exception("{}".format(msg[1:]))
                elif msg[0] == "4":
                    offset.append(msg[1:])
                else:
                    if msg[0] == "1":
                        # 1 is a entry
                        messages.append(loads(msg[1:]))
        data["messages"] = messages
        data["offset"] = offset
        if extract_pandas == 0:
            # In this case return a dictionary with two keys , one is the messages and the other one is the offset
            return data
        else:
            # In this case we parse to ConvertToDF the messages and the we get the value of every message
            # next we return a pandas data frame of them
            return self._ConvertToDF(data["messages"])


    def _ConvertToDF(self, data):
        """
        Get data from sql handler and extract from generate dict the messages and then the dict-value from each one

        :param data: list of dictionaries
        :return: pandas dataframe
        """
        data = list(map(lambda x: loads(x["value"]), data))
        if len(self.datetimelist) > 0 and len(self.formatinglist) > 0:
            # If these two lists has length greater than zero , then call class ConvertDateTime which
            # which convert specific keys ,which have datetime string to datetime object
            # this convert can be only if user request data as pandas dataframe
            data = ConvertDateTime(data, self.datetimelist, self.formatinglist).Convert()
        # Convert data to pandas dataframe
        data = pd.DataFrame(data)
        return data

    # def _ConvertToDF(self, data):
    #     """
    #     Get data from sql handler and extract from generate dict the messages and then the dict-value from each one
    #     :param data: dictionary
    #     :return: pandas dataframe
    #     """
    #     # Data has two keys messages/data and offsets
    #     if data.get("messages", None) is not None:
    #         data = data["messages"]
    #         key = "messages"
    #     elif data.get("data", None) is not None:
    #         data = data["data"]
    #         key = "data"
    #     else:
    #         raise Exception("There isn't key messages or data to exctract values for create pandas dataframe.\n")
    #     # Get key value , which is a stringfy dict, loads convert it to dict
    #     if key != "data":
    #         data = list(map(lambda x: loads(x["value"]), data))
    #     else:
    #         data = list(map(lambda x: loads(x["value"]), data["messages"]))
    #         temp_list = []
    #
    #     if len(self.datetimelist) > 0 and len(self.formatinglist) > 0:
    #         # If these two lists has length greater than zero , then call class ConvertDateTime which
    #         # which convert specific keys ,which have datetime string to datetime object
    #         # this convert can be only if user request data as pandas dataframe
    #         data = ConvertDateTime(data, self.datetimelist, self.formatinglist).Convert()
    #     # Convert data to dataframe
    #     data = pd.DataFrame(data)
    #     return data








