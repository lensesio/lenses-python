from requests import get, delete, post, put
from lenses_python.constants import PROCESSOR_END_POINT


class ProcessorHandler:

    def __init__(self, url, username, password, token):
        self.url = url
        self.processor_end_point = PROCESSOR_END_POINT
        self.username = username
        self.password = password
        self.token = token
        self.default_headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                                'x-kafka-lenses-token': self.token}

    def CreateProcessor(self, name, sql, runners, clusterName, namespace, pipeline):
        """
        need changes look here  http://lenses.stream/lenses-sql/index.html#kubernetes
        POST /api/streams

        :param name: string
        :param sql:string, query
        :param runners:int
        :param clusterName:string
        :param namespace:string
        :param pipeline:string, applies for Kubernetes mode
        :return:LSQL id,string, applies for Kubernetes mode
        """
        url = self.url+self.processor_end_point
        if namespace != "" and pipeline != "":
            # in case we want to applt for Kubernetes mode
            params = dict(
                name=name,
                sql=sql,
                runners=runners,
                clusterName=clusterName,
                namespace=namespace,
                pipeline=pipeline
                         )
        else:
            params = dict(
                name=name,
                sql=sql,
                runners=runners,
                clusterName=clusterName
            )


        headers = {'Content-Type': 'application/json', 'Accept': 'text/plain',
                    'x-kafka-lenses-token': self.token}
        response = post(url, headers=headers, json=params)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code,response.text))
        return response.text

    def PauseProcessor(self, processorName):
        """

        PUT /api/streams/(string: processorName)/pause

        :param processorName:LSQL Stream id
        :return:
        """
        url = self.url+self.processor_end_point+"/"+processorName+"/pause"
        response = put(url,  headers=self.default_headers)

        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))

    def ResumeProcessor(self, processorName):
        """
        PUT /api/streams/(string: processorName)/resume

        :param processorName: LSQL Stream id
        :return:
        """
        url = self.url+self.processor_end_point+"/"+processorName+"/resume"
        response = put(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))

    def UpdateProcessorRunners(self, processorName, numberOfRunners):
        """
        PUT /api/streams/(string: processorName)/scale/(int: numberOfRunners)

        :param processorName:LSQL Stream id
        :param numberOfRunners:string
        :return:
        """
        url = self.url+self.processor_end_point+"/"+processorName+"/scale/"+numberOfRunners
        response = put(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))

    def DeleteProcessor(self, processorName):
        """
        DELETE /api/streams/(string: processorName)

        :param processorName:LSQL Stream id
        :return:
        """
        url = self.url+self.processor_end_point+"/"+processorName
        response = delete(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
