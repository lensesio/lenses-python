from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request
from threading import Thread, enumerate, RLock
from queue import Queue
import inspect
import json
import sys
import websocket
import ssl


class SQLExec:

    def __init__(self, active_threads, verify_cert=True):
        getEndpoints.__init__(self, "sqlEndpoints")

        self.verify_cert=verify_cert
        self.lenses_sql_query_validation = self.url + self.lensesSQLValidationEndpoint
        self.lenses_exec_sql_endpoint = self.url + self.lensesSQLEndpoint
        self.sql_headers = {
            'Content-Type': 'text/event-stream',
            'Accept': 'text/event-stream',
            'x-kafka-lenses-token': self.token
        }

        self.active_threads = active_threads

    def ValidateSqlQuery(self):
        self.validateSqlQuery = exec_request(
            __METHOD="get",
            __EXPECTED="text",
            __URL=self.lenses_sql_query_validation,
            __HEADERS=self.active_headers,
            __DATA=self.params,
            __VERIFY=self.verify_cert
        )

        if 'Expected one of Alter' in self.validateSqlQuery:
            raise Exception(self.validateSqlQuery.text)

    def SQL(self, query, spawn_thread=None, stats=0):
        if spawn_thread:
            sqlQue=Queue()
            self.new_sql = Thread(
                target=self.ExecSQL,
                args=(
                    query,
                    stats,
                    sqlQue,
                ),
                daemon=False
            )

            self.active_threads['thread_lock'].acquire()
            self.active_threads['sql']['t'] += 1
            t = self.active_threads['sql']['t']
            self.active_threads['sql'][t] = {
                'query': query,
                'sqlQue': sqlQue,
                'stats': stats,
            }

            self.active_threads['thread_lock'].release()
            self.new_sql.start()

            print(
                "SQL Thread -\t with SQL_ID: %s has been started\n" % t,
                "You may find thread info at .active_threads object\n",
                "To access the thread's data, use the thread's queue:\n"
                "\tcallers_name.active_theads['sql'][SQL_ID][sqlQueu]\n",
                "Example how to get data from a thread's queue with soft lock:\n",
                "\tcallers_name.active_theads['sql'][SQL_ID]['sqlQueu'].get(block=True, timeout=5)\n"
            )
        elif spawn_thread is None:
            self.execSQL = self.ExecSQL(query, stats, sqlQue=None)
            return self.execSQL


    def ExecSQL(self, query, stats=0, sqlQue=None):
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

        if self.verify_cert:
            conn = websocket.create_connection(
                self.lenses_exec_sql_endpoint
            )
        else:
            conn = websocket.create_connection(
                self.lenses_exec_sql_endpoint,
                sslopt={"cert_reqs": ssl.CERT_NONE}
            )

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
            execSQL = {
                'data': data_list,
                'stats': stats_list,
                'metadata': sql_metadata,
                'ERROR': sql_error
            }

            if sqlQue:
                sqlQue.put(execSQL)
            else:
                return execSQL
        except KeyboardInterrupt:
            conn.close()
        except:
            conn.close()
            raise
