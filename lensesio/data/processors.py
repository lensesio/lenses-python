from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request


class DataProcessor:

    def __init__(self, verify_cert=True):
        getEndpoints.__init__(self, "processorEndpoints")

        self.verify_cert=verify_cert
        self.processor_end_point = self.url + self.lensesProcessorsEndpoint
        self.processor_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'x-kafka-lenses-token': self.token
        }

    def CreateProcessor(self, name, sql, runners, clusterName, namespace=None, pipeline=None):
        """

        :param name: string
        :param sql:string, query
        :param runners:int
        :param clusterName:string
        :param namespace:string
        :param pipeline:string, applies for Kubernetes mode
        """
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'text/plain',
            'x-kafka-lenses-token': self.token
        }

        params = dict(
            name=name,
            sql=sql,
            runners=runners,
            clusterName=clusterName
        )

        self.createProcessor = exec_request(
            __METHOD="post",
            __EXPECTED="text",
            __URL=self.processor_end_point,
            __HEADERS=headers,
            __DATA=params,
            __VERIFY=self.verify_cert
        )

        return self.createProcessor

    def GetProcessors(self):
        self.getProcessors = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=self.processor_end_point,
            __HEADERS=self.processor_headers,
            __VERIFY=self.verify_cert
        )

        return self.getProcessors

    def GetProcessorID(self, processor_name):
        self.GetProcessors()

        self.getProcessorID = []
        for p in self.getProcessors['streams']:
            try:
                if p['name'] == processor_name:
                    self.getProcessorID.append(p['id'])

            except KeyError:
                pass

        return self.getProcessorID

    def PauseProcessor(self, processorName):
        """

        :param processorName:LSQL Stream id
        """
        __RQE = self.processor_end_point
        __RQE = __RQE + '/' + processorName + '/pause'
        self.pauseProcessor = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.processor_headers,
            __VERIFY=self.verify_cert
        )

        return self.pauseProcessor

    def ResumeProcessor(self, processorName):
        """

        :param processorName: LSQL Stream id
        """
        __RQE = self.processor_end_point
        __RQE = __RQE + '/' + processorName + '/resume'
        self.resumeProcessor = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.processor_headers,
            __VERIFY=self.verify_cert
        )

        return self.resumeProcessor

    def UpdateProcessorRunners(self, processorName, numberOfRunners):
        """

        :param processorName:LSQL Stream id
        :param numberOfRunners:string
        """
        __RQE = self.processor_end_point
        __RQE = __RQE + '/' + processorName + '/scale/'
        __RQE = __RQE + numberOfRunners
        self.updateProcessorRunners = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.processor_headers,
            __VERIFY=self.verify_cert
        )

        return self.updateProcessorRunners

    def DeleteProcessor(self, processorName):
        """

        :param processorName:LSQL Stream id
        """
        __RQE = self.processor_end_point
        __RQE = __RQE + '/' + processorName
        self.deleteProcessor = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.processor_headers,
            __VERIFY=self.verify_cert
        )

        return self.deleteProcessor
