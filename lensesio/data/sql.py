from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request
import json
import websocket


class SQLExec:

    def __init__(self):
        getEndpoints.__init__(self, "sqlEndpoints")

        self.lenses_sql_query_validation = self.url + self.lensesSQLValidationEndpoint
        self.lenses_exec_sql_endpoint = self.url + self.lensesSQLEndpoint
        self.sql_headers = {
            'Content-Type': 'text/event-stream',
            'Accept': 'text/event-stream',
            'x-kafka-lenses-token': self.token
        }

    def ValidateSqlQuery(self):
        self.validateSqlQuery = exec_request(
            __METHOD="get",
            __EXPECTED="text",
            __URL=self.lenses_sql_query_validation,
            __HEADERS=self.active_headers,
            __DATA=self.params
        )

        if 'Expected one of Alter' in self.validateSqlQuery:
            raise Exception(self.validateSqlQuery.text)

    def ExecSQL(self, query, stats=0):
        """

        :param is_live:
        :param stats: int
        :return: In case is_extract if false return a dictionary,
        otherwise return Pandas dataframe
        """
        self.query = query
        self.params = {'sql': self.query}
        self.ValidateSqlQuery()

        if 'https' in self.lenses_exec_sql_endpoint:
            self.lenses_exec_sql_endpoint = self.lenses_exec_sql_endpoint.replace(
                "https", "wss"
            )
        else:
            self.lenses_exec_sql_endpoint = self.lenses_exec_sql_endpoint.replace(
                "http", "ws"
            )

        conn = websocket.create_connection(self.lenses_exec_sql_endpoint)
        message = {
            "token": self.token,
            "sql": self.query,
        }

        try:
            conn.send(json.dumps(message))

            data_list = []
            stats_list = []
            sql_metadata = []
            sql_error = []

            while True:
                received_data = conn.recv()
                if not received_data or received_data == '':
                    break

                __data = json.loads(received_data)
                __data_type = __data.get("type", None)

                if __data_type is None:
                    raise KeyError("There isn't key 'type'")
                if __data_type == "RECORD":
                    data_list.append(__data["data"])
                elif __data_type == "STATS":
                    stats_list.append(__data["data"])
                elif __data_type == "END":
                    break
                elif __data_type == "METADATA":
                    sql_metadata = __data["data"]
                elif __data_type == "ERROR":
                    sql_error.append(__data["data"])

            conn.close()
            self.execSQL = {
                'data': data_list,
                'stats': stats_list,
                'metadata': sql_metadata,
                'ERROR': sql_error
            }

            return self.execSQL
        except KeyboardInterrupt:
            conn.close()
