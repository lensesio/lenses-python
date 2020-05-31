from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request


class KafkaQuotas:

    def __init__(self, verify_cert=True):
        getEndpoints.__init__(self, "quotaEndpoints")

        self.verify_cert=verify_cert
        self.quotas_end_point = self.url + self.lensesQuotasEndpoint
        self.quotas_users_end_point = self.url + self.lensesUserQuotasEndpoint
        self.quotas_clients_end_point = self.url + self.lensesClientQuotasEndpoint
        self.quota_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'x-kafka-lenses-token': self.token
        }

    def GetQuotas(self):
        self.getQuotas = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=self.quotas_end_point,
            __HEADERS=self.quota_headers,
            __VERIFY=self.verify_cert
        )

        return self.getQuotas

    def SetQuotasAllUsers(self, config):
        """

        :param config:
        {
            "producer_byte_rate" : "100000",
            "consumer_byte_rate" : "200000",
            "request_percentage" : "75"
        }
        """
        self.setQuotasAllUsers = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=self.quotas_users_end_point,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.setQuotasAllUsers

    def SetQuotaUserAllClients(self, user, config):
        """

        :param user:The user to set the quota for
        :param config:
        {
            "producer_byte_rate" : "100000",
            "consumer_byte_rate" : "200000",
            "request_percentage" : "75"
        }
        The quota contrain
        """
        __RQE = self.quotas_users_end_point
        __RQE = __RQE + '/' + user + '/clients'
        self.setQuotaUserAllClients = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.setQuotaUserAllClients

    def SetQuotaUserClient(self, user, clientid, config):
        """

        :param user:The user to set the quota for
        :param clientid:The client id to set the quota for
        :param config:The quota contraints
        {
            "producer_byte_rate" : "100000",
            "consumer_byte_rate" : "200000",
            "request_percentage" : "75"
        }
        """
        __RQE = self.quotas_users_end_point
        __RQE = __RQE + '/' + user + '/clients/'
        __RQE = __RQE + clientid
        self.setQuotaUserClient = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.setQuotaUserClient

    def SetQuotaUser(self, user, config):
        """

        :param user:The user to set the quota for
        :param config: The quota contraints
        {
            "producer_byte_rate" : "100000",
            "consumer_byte_rate" : "200000",
            "request_percentage" : "75"
        }
        """
        __RQE = self.quotas_users_end_point
        __RQE = __RQE + '/' + user
        self.setQuotaUser = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.setQuotaUser

    def SetQuotaAllClient(self, config):
        """

        :param config:The quota contraints,
        {
            "producer_byte_rate" : "100000",
            "consumer_byte_rate" : "200000",
            "request_percentage" : "75"
        }
        """
        self.setQuotaAllClient = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=self.quotas_clients_end_point,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.setQuotaAllClient

    def SetQuotaClient(self, clientid, config):
        """

        :param clientid:The client id to set the quota for
        :param config: The quota contraints,
        {
            "producer_byte_rate" : "100000",
            "consumer_byte_rate" : "200000",
            "request_percentage" : "75"
        }
        """
        __RQE = self.quotas_clients_end_point
        __RQE = __RQE + '/' + clientid
        self.setQuotaClient = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.setQuotaClient

    def DeleteQutaAllUsers(self, config):
        """

        :param config: A list we the parameters which want to delete
        """
        self.deleteQutaAllUsers = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=self.quotas_users_end_point,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.deleteQutaAllUsers

    def DeleteQuotaUserAllClients(self, user, config):
        """

        :param user:
        :param config: A list we the parameters which want to delete
        """
        __RQE = self.quotas_users_end_point
        __RQE = __RQE + '/' + user + '/clients'
        self.deleteQuotaUserAllClients = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.deleteQuotaUserAllClients

    def DeleteQuotaUserClient(self, user, clientid, config):
        """

        :param user:The user to set the quota for
        :param clientid:The client id to set the quota for
        :param config: A list we the parameters which want to delete
        """
        __RQE = self.quotas_users_end_point
        __RQE = __RQE + '/' + user + '/clients/'
        __RQE = __RQE + clientid
        self.deleteQuotaUserClient = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.deleteQuotaUserClient

    def DeleteQuotaUser(self, user, config):
        """

        :param user:The user to set the quota for
        :param config: A list we the parameters which want to delete
        """
        __RQE = self.quotas_users_end_point
        __RQE = __RQE + '/' + user
        self.deleteQuotaUser = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.deleteQuotaUser

    def DeleteQuotaAllClients(self, config):
        """

        :param config: A list we the parameters which want to delete
        """
        self.deleteQuotaAllClients = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=self.quotas_clients_end_point,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.deleteQuotaAllClients

    def DeleteQuotaClient(self, clientid, config):
        """

        :param clientid: The client id to set the quota for
        :param config: A list we the parameters which want to delete
        """
        __RQE = self.quotas_clients_end_point
        __RQE = __RQE + '/' + clientid
        self.deleteQuotaClient = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.quota_headers,
            __DATA=config,
            __VERIFY=self.verify_cert
        )

        return self.deleteQuotaClient
