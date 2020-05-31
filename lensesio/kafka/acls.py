from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request


class KafkaACL():

    def __init__(self, verify_cert=True):
        getEndpoints.__init__(self, "aclEndpoints")

        self.verify_cert=verify_cert
        self.lenses_acl_endpoint = self.url + self.lensesAclEndpoint
        self.acls_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'x-kafka-lenses-token': self.token
        }

    def GetAcl(self):
        """

        :return: Return ACLs
        """
        self.getACL = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=self.lenses_acl_endpoint,
            __HEADERS=self.acls_headers,
            __VERIFY=self.verify_cert
        )

        return self.getACL

    def SetAcl(self, resourceType, resourceName, principal, permissionType, host, operation):
        """

        :param resourceType: string, required
        :param resourceName: string, required
        :param principal: string, required
        :param permissionType:  string, required (either Allow or Deny)
        :param host: string, required
        :param operation:  string, required
        """
        params = dict(
            resourceType=resourceType,
            resourceName=resourceName,
            principal=principal,
            permissionType=permissionType,
            host=host,
            operation=operation
        )

        self.setAcl = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=self.lenses_acl_endpoint,
            __HEADERS=self.acls_headers,
            __DATA=params,
            __VERIFY=self.verify_cert
        )

        return self.setAcl

    def DelAcl(self, resourceType, resourceName, principal, permissionType, host, operation):
        """

        :param resourceType: string, required
        :param resourceName: string, required
        :param principal: string, required
        :param permissionType:  string, required (either Allow or Deny)
        :param host: string, required
        :param operation:  string, required
        """
        params = dict(
            resourceType=resourceType,
            resourceName=resourceName,
            principal=principal,
            permissionType=permissionType,
            host=host,
            operation=operation,
            selectedColumns=[]
        )

        self.setAcl = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=self.lenses_acl_endpoint,
            __HEADERS=self.acls_headers,
            __DATA=params,
            __VERIFY=self.verify_cert
        )

        return self.setAcl
