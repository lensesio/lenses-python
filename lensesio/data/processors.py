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

#{"cluster":"k8-lenses-domain","name":"dev","runnerCount":1,"sql":"INSERT INTO devtopic001 SELECT TABLE * FROM backblaze_smart","settings":{},"namespace":"se-instance","pipeline":"a"}
    def CreateProcessor(self, name, sql, clusterName="IN_PROC", namespace=None, pipeline=None, runnerCount=1):
        """

        :param name: string
        :param sql:string, query
        :param runnerCount:int
        :param clusterName:string
        :param namespace:string
        :param pipeline:string, applies for Kubernetes mode
        """

        params = dict(
            name=name,
            sql=sql,
            runnerCount=str(runnerCount),
            cluster=clusterName,
        )

        if namespace != None:
            params["namespace"] = namespace

        if pipeline != None:
            params["pipeline"] = pipeline

        self.createProcessor = exec_request(
            __METHOD="post",
            __EXPECTED="text",
            __URL=self.processor_end_point,
            __HEADERS=self.processor_headers,
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
        __RQE = __RQE + '/' + processorName + '/stop'
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
