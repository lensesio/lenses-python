from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request


class DataConnector:

    def __init__(self, verify_cert=True):
        getEndpoints.__init__(self, "connectEndpoints")

        self.verify_cert=verify_cert
        self.lenses_connect_endpoint = self.url + self.lensesConnectEndpoint
        self.connector_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'x-kafka-lenses-token': self.token
        }

    def GetConnectors(self, cluster):
        """

        :param cluster:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors'
        self.getConnectors = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.getConnectors

    def GetConnectorInfo(self, cluster, connector):
        """

        :param cluster:
        :param connector:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/'
        __RQE = __RQE + connector
        self.getConnectorInfo = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.getConnectorInfo

    def GetConnectorConfig(self, cluster, connector):
        """

        :param cluster:
        :param connector:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/'
        __RQE = __RQE + connector + '/config'
        self.getConnectorConfig = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.getConnectorConfig

    def GetConnectorStatus(self, cluster, connector):
        """

        :param cluster:
        :param connector:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/'
        __RQE = __RQE + connector + '/status'
        self.getConnectorStatus = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.getConnectorStatus

    def GetConnectorTasks(self, cluster, connector):
        """

        :param cluster:
        :param connector:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/'
        __RQE = __RQE + connector + '/tasks'
        self.getConnectorTasks = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.getConnectorTasks

    def GetStatusTask(self, cluster, connector, task_id):
        """

        :param cluster:
        :param connector:
        :param task_id:string of id.
        For example if we have something like that
        'id': {'connector': 'FTP', 'task': 0}
        the task_id is '0'
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/'
        __RQE = __RQE + connector + '/tasks/' + task_id + '/status'
        self.getStatusTask = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.getStatusTask

    def RestartConnectorTask(self, cluster, connector, task_id):
        """

        :param cluster:
        :param connector:
        :param task_id:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/'
        __RQE = __RQE + connector + '/tasks/' + task_id + '/restart'
        self.restartConnectorTask = exec_request(
            __METHOD="post",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.restartConnectorTask

    def GetConnectorPlugins(self, cluster):
        """

        :param cluster:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connector-plugins'
        self.getConnectorPlugins = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.getConnectorPlugins

    def PauseConnector(self, cluster, connector):
        """

        :param cluster:
        :param connector:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/'
        __RQE = __RQE + connector + '/pause'
        self.pauseConnector = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.pauseConnector

    def ResumeConnector(self, cluster, connector):
        """

        :param cluster:
        :param connector:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/'
        __RQE = __RQE + connector + '/resume'
        self.resumeConnector = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.resumeConnector

    def RestartConnector(self, cluster, connector):
        """

        :param cluster:
        :param connector:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/'
        __RQE = __RQE + connector + '/restart'
        self.restartConnector = exec_request(
            __METHOD="post",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.restartConnector

    def CreateConnector(self, cluster, config):
        """
        class = 'com.datamountaineer.streamreactor.connect.coap.sink.CoapSinkConnector'
        :param cluster:
        :param config:{
            'config':{
                'connect.coap.kcql': '1',
                'connector.class': class
            },
            'name': 'name'
        }
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors'
        self.createConnector = exec_request(
            __METHOD="post",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.createConnector

    def SetConnectorConfig(self, cluster, connector, config):
        """

        :param cluster:
        :param connector:
        :param config: For example {
            'connector.class': 'org.apache.kafka.connect.file.FileStreamSinkConnector',
            'task.max': 5,
            'topics': 'nyc_yellow_taxi_trip_data,reddit_posts,sea_vessel_position_reports,
            telecom_italia_data',
            'file': '/dev/null',
            'tasks.max': '4',
            'name': 'nullsink'
        }
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/'
        __RQE = __RQE + connector + '/config'
        self.setConnectorConfig = exec_request(
            __METHOD="put",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.setConnectorConfig

    def DeleteConnector(self, cluster, connector):
        """

        :param cluster:
        :param connector:
        """
        __RQE = self.lenses_connect_endpoint
        __RQE = __RQE + '/' + cluster + '/connectors/' + connector
        self.deleteConnector = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.connector_headers,
            __VERIFY=self.verify_cert
        )

        return self.deleteConnector
