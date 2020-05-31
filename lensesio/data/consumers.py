from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request


class DataConsumers:

    def __init__(self, verify_cert=True):
        getEndpoints.__init__(self, "consumersEndpoints")

        self.verify_cert=verify_cert
        self.lenses_consumers_endpoint = self.url + self.lensesConsumersEndpoint
        self.consumers_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'x-kafka-lenses-token': self.token
        }

    def GetConsumers(self):
        __RQE = self.lenses_consumers_endpoint
        self.getConsumers = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.consumers_headers,
            __VERIFY=self.verify_cert
        )

        return self.getConsumers

    def GetConsumersNames(self):
        request = self.GetConsumers()
        self.consumerNameList = []

        for c in request:
            if c['id'] in ['', ' ']:
                self.consumerNameList.append('UNKNOWN')
            else:
                self.consumerNameList.append(c['id'])

        return self.consumerNameList
