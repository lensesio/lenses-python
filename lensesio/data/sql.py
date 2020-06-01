from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request
from threading import Thread, enumerate, RLock
import queue
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

        self.sql_active_threads = active_threads

    def stop_sql(self, t, stop=False):
        if stop and self.sql_active_threads['sql'][t]['state']:
            print("SQL Thread has been marked for shutdown")
            self.sql_active_threads['sql'][t]['state'] = False

        self.sql_state_report(t)
    
    def sql_state_report(self, t):
        sqlthread = self.sql_active_threads['sql'][t]['thread']
        if self.sql_active_threads['sql'][t]['state'] and sqlthread.is_alive():
            msg = "SQL Thread is running"
        elif not self.sql_active_threads['sql'][t]['state'] and sqlthread.is_alive():
            msg = "Waiting for thread to shutdown"
        else:
            msg = "SQL Thread has closed"

        return msg, self.sql_active_threads['sql'][t]['state']

    def consume_sql_queue(self, t):
        try:
            data = self.sql_active_threads['sql'][t]['sqlQue'].get(block=False)
            return data, True
        except queue.Empty:
            return "Empty Queue", False

    def SQL(self, query, spawn_thread=None, stats=0):
        if spawn_thread:

            self.sql_active_threads['thread_lock'].acquire()
            self.sql_active_threads['sql']['t'] += 1
            t = self.sql_active_threads['sql']['t']
            self.sql_active_threads['thread_lock'].release()

            sqlQue=queue.Queue()
            self.new_sql = Thread(
                target=self.ExecSQL,
                args=(
                    query,
                    stats,
                    sqlQue,
                    t,
                ),
                daemon=False
            )

            self.sql_active_threads['thread_lock'].acquire()
            self.sql_active_threads['sql'][t] = {
                'query': query,
                'sqlQue': sqlQue,
                'stats': stats,
                "state": False,
                "state_info": None,
                "thread": self.new_sql
            }

            self.new_sql.start()
            self.sql_active_threads['thread_lock'].release()

            print(
                "SQL Thread -\t with SQL_ID: %s has been started\n" % t,
                "You may find thread info at .active_threads object\n",
                "To access the thread's data, use the thread's queue:\n"
                "\tcallers_name.active_theads['sql'][SQL_ID][sqlQueu]\n",
                "Example how to get data from a thread's queue with soft lock:\n",
                "\tcallers_name.active_theads['sql'][SQL_ID]['sqlQueu'].get(block=True, timeout=5)\n"
            )
        elif spawn_thread is None:
            self.execSQL = self.ExecSQL(query, stats, sqlQue=None, t=None)
            return self.execSQL


    def ExecSQL(self, query, stats=0, sqlQue=None, t=None):
        """

        :param is_live:
        :param stats: int
        :return: In case is_extract if false return a dictionary,
        otherwise return Pandas dataframe
        """
        def update_thread_state(msg, state=True):
            if sqlQue:
                self.sql_active_threads['thread_lock'].acquire()
                self.sql_active_threads['sql'][t]['state'] = state
                self.sql_active_threads['sql'][t]['state_info'] = msg
                self.sql_active_threads['thread_lock'].release()

        update_thread_state("Started")
        self.query = query
        self.params = {'sql': self.query}

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

        update_thread_state("Created websocket client")
        message = {
            "token": self.token,
            "sql": self.query,
        }

        try:
            conn.send(json.dumps(message))
            update_thread_state("Executing query")
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
                update_thread_state("Query execution finished successfully", False)
            else:
                return execSQL
        except KeyboardInterrupt:
            update_thread_state("")
            conn.close()
        except:
            update_thread_state(sys.exc_info(), False)
            conn.close()
            raise
