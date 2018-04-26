from requests import *
from lenses_python.ReadConfigFile import ReadConfigFile

class ConnectorHandler:

    def __init__(self, url, username, password, token):
        self.url = url
        self.username = username
        self.password = password
        self.token = token
        self.default_headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                            'x-kafka-lenses-token': self.token}

    def ListAllConnectors(self, cluster):
        """
        List active connectors
        GET /api/proxy-connect/(string: clusterAlias)/connectors
        :param cluster:
        :return:
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors"
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def GetInfoConnector(self, cluster, connector):
        """
        Get information about a specific connector
        GET /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)

        :param cluster:
        :param connector:
        :return: Example of return {'tasks': [{'connector': 'FTP', 'task': 0}],
                                            'config': {'connect.ftp.monitor.tail': '/logs/:cc_data',
                                            'connect.ftp.password': 'x',
                                            'connect.ftp.user': 'x',
                                            'connect.ftp.refresh': '30PT1',
                                            'connect.ftp.file.maxage': '30PT1',
                                            'connect.ftp.keystyle': 'struct',
                                            'connector.class': 'com.datamountaineer.streamreactor.connect.ftp.
                                            source.FtpSourceConnector',
                                           'name': 'FTP',
                                           'connect.ftp.address': 'x'},
                                         'name': 'FTP'}
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def GetConnectorConfig(self, cluster, connector):
        """
        Get connector config
        GET /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)/config
        :param cluster:
        :param connector:
        :return: Example of return {'connect.ftp.monitor.tail': '/logs/:cc_data',
                                    'connect.ftp.password': 'x',
                                    'connect.ftp.user': 'x',
                                    'connect.ftp.refresh': '30PT1',
                                    'connect.ftp.file.maxage': '30PT1',
                                    'connect.ftp.keystyle': 'struct',
                                    'connector.class': 'com.datamountaineer.streamreactor.connect.ftp.source.
                                    FtpSourceConnector',
                                    'name': 'FTP',
                                    'connect.ftp.address': 'x'}

        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector+"/config"
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def GetConnectorStatus(self, cluster, connector):
        """
        Get connector status
        GET /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)/status
        :param cluster:
        :param connector:
        :return:
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector+"/status"
        response = get(url,headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def GetConnectorTasks(self, cluster, connector):
        """
        Get list of connector tasks
        GET /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)/tasks
        :param cluster:
        :param connector:
        :return:
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector+"/tasks"
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def GetStatusTask(self, cluster, connector, task_id):
        """
        Get current status of a task
        GET /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)/tasks/(string: task_id)/status

        :param cluster:
        :param connector:
        :param task_id:string of id. For example if we have something like that   'id': {'connector': 'FTP', 'task': 0}
        the task_id is '0'
        :return:
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector+"/tasks/"+task_id+"/status"
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def RestartConnectorTask(self, cluster, connector, task_id):
        """
        Restart a connector task
        POST /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)/tasks/(string: task_id)
        /restart
        :param cluster:
        :param connector:
        :param task_id:
        :return:
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector+"/tasks/"+task_id+"/restart"
        response = post(url, headers=self.default_headers)
        if response.status_code not in [200, 204]:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))

    def GetConnectorPlugins(self, cluster):
        """
        List available connector plugins
        GET /api/proxy-connect/(string: clusterAlias)/connector-plugins
        :param cluster:
        :return:
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connector-plugins"
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def PauseConnector(self, cluster, connector):
        """
        Pause a connector
        PUT /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)/pause

        :param cluster:
        :param connector:
        :return:
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector+"/pause"
        response = put(url, headers=self.default_headers)
        if response.status_code not in [200, 202]:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        # return response.json()

    def ResumeConnector(self, cluster, connector):
        """
        Resume a paused connector
        PUT /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)/resume
        :param cluster:
        :param connector:
        :return:
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector+"/resume"
        response = put(url, headers=self.default_headers)
        if response.status_code not in [200, 202]:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        # return response.json()

    def RestartConnector(self, cluster, connector):
        """
        Restart a connector
        POST /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)/restart
        :param cluster:
        :param connector:
        :return:
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector+"/restart"
        response = post(url, headers=self.default_headers)
        if response.status_code not in [200, 202, 204]:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        # return response.json()

    def CreateConnector(self, cluster, config, filename):
        """
        Create new connector
        POST /api/proxy-connect/(string: clusterAlias)/connectors [CONNECTOR_CONFIG]
        :param cluster:
        :param config:{'config': {'connect.coap.kcql': '1',
                        'connector.class': 'com.datamountaineer.streamreactor.connect.coap.sink.CoapSinkConnector'},
                         'name': 'name'}
        :param filename: name of file where we save a json/dict like the config above

        :return:
        """
        if cluster == "" and config == "" and filename != "":
            # Check if file is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("cluster", None) is not None and temp_dict.get("config", None) is not None:
                cluster = temp_dict["cluster"]
                config = temp_dict["config"]
            else:
                raise Exception("In file there aren't sections cluster and config\n")
        elif filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("config", None) is not None:
                # The option name of option must be config and have json format
                config = temp_dict["config"]
            else:
                raise Exception("In file there isn't section config\n")
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors"
        response = post(url, headers=self.default_headers, json=config)
        if response.status_code not in [200, 201]:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def SetConnectorConfig(self, cluster, connector, config, filename):
        """
        Set connector config
        PUT /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)/config
        :param cluster:
        :param connector:
        :param config: For example {'connector.class': 'org.apache.kafka.connect.file.FileStreamSinkConnector',
                                    'task.max': 5,
                                    'topics': 'nyc_yellow_taxi_trip_data,reddit_posts,sea_vessel_position_reports,
                                    telecom_italia_data',
                                    'file': '/dev/null',
                                    'tasks.max': '4',
                                    'name': 'nullsink'}
        :param filename: name of file where we save a json/dict like the config above
        :return:
        """
        if cluster == "" and connector == "" and config == "" and filename != "":
            # Check if filename id not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("cluster", None) is not None and temp_dict.get("connector", None) is not None and temp_dict.get("config", None) is not None:
                cluster = temp_dict["cluster"]
                connector = temp_dict["connector"]
                config = temp_dict["config"]
            else:
                raise Exception("In file there aren't sections cluster, connector and config\n")
        elif filename != "":
            # Check if filename is not empty
            temp_dict = ReadConfigFile(filename).GetJSONs()
            if temp_dict.get("config", None) is not None:
                # The option name of option must be config and have json format
                config = temp_dict["config"]
            else:
                raise Exception("In file there isn't section config\n")
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector+"/config"
        response = put(url, headers=self.default_headers, json=config)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def DeleteConnector(self, cluster, connector):
        """
        Remove a running connector
        DELETE /api/proxy-connect/(string: clusterAlias)/connectors/(string: connectorName)
        :param cluster:
        :param connector:
        :return:
        """
        url = self.url+"/api/proxy-connect/"+cluster+"/connectors/"+connector
        response = delete(url, headers=self.default_headers)
        if response.status_code not in [200, 204]:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))








