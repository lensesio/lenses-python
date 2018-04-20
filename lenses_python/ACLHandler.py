from requests import *

class ACLHandler:

    def __init__(self, token, url):
        self.token = token
        self.url = url
        self.url_extend = "/api/acl"
        self.default_headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                                'x-kafka-lenses-token': self.token}

    def GetACL(self):
        """

        :return: Return ACLs
        """

        url = self.url+self.url_extend
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def SetAcl(self, resourceType, resourceName, principal, permissionType, host, operation):
        """
        Create/Update ACLs
        :param resourceType: string, required
        :param resourceName: string, required
        :param principal: string, required
        :param permissionType:  string, required (either Allow or Deny)
        :param host: string, required
        :param operation:  string, required
        :return:
        """
        url = self.url+self.url_extend
        params = dict(
            resourceType=resourceType,
            resourceName=resourceName,
            principal=principal,
            permissionType=permissionType,
            host=host,
            operation=operation
        )
        response = put(url, json=params, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
