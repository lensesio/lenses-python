from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request


class Topology:

    def __init__(self, verify_cert=True):
        getEndpoints.__init__(self, "topologyEndpoints")

        self.verify_cert=verify_cert
        self.lenses_topology_endpoint = self.url + self.lensesTopologyEndpoint
        self.topology_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json text/plain',
            'x-kafka-lenses-token': self.token
        }

    def GetTopology(self):
        __RQE = self.lenses_topology_endpoint
        self.getTopology = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.topology_headers,
            __VERIFY=self.verify_cert
        )

        return self.getTopology
