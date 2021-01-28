from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request


class AdminPanel:

    def __init__(self, verify_cert=True):
        getEndpoints.__init__(self, "adminEndpoint")

        self.verify_cert=verify_cert
        self.admin_config_info_endpoint = self.url + self.lensesConfigInfoEndpoint
        self.admin_audits_endpoint = self.url + self.lensesAdminAuditsEndpoint
        self.admin_alerts_endpoint = self.url + self.lensesAdminAlertsEndpoint
        self.admin_logs_endpoint = self.url + self.lensesLogsEndpoint
        self.admin_groups_endpoint = self.url + self.lensesGroupsEndpoint
        self.admin_users_endpoint = self.url + self.lensesUsersEndpoint
        self.admin_serviceaccounts_endpoint = self.url + self.lensesServiceAccountEndpoint

        self.admin_x_headers = {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json',
                                'x-kafka-lenses-token': self.token
        }
        self.admin_text_plain_headers = {
                                        'Content-Type': 'application/json',
                                        'Accept': 'text/plain',
                                        'x-kafka-lenses-token': self.token
        }

    def GetGroups(self):
        __RQE = self.admin_groups_endpoint
        self.getGroups = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __VERIFY=self.verify_cert
        )

        return self.getGroups

    def CreateGroup(self, payload):
        __RQE = self.url + "/api/v1/group"
        self.createGroup = exec_request(
            __METHOD="post",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __DATA=payload,
            __VERIFY=self.verify_cert
        )

        return self.createGroup

    def UpdateGroup(self, group, payload):
        __RQE = self.url + "/api/v1/group/" + group
        self.updateGroup = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.admin_text_plain_headers,
            __DATA=payload,
            __VERIFY=self.verify_cert
        )

        return self.updateGroup

    def DeleteGroup(self, group):
        __RQE = self.url + "/api/v1/group/" + group
        self.deleteGroup = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.admin_text_plain_headers,
            __VERIFY=self.verify_cert
        )

        return self.deleteGroup

    def GetUsers(self):
        __RQE = self.admin_users_endpoint
        self.getUsers = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __VERIFY=self.verify_cert
        )

        return self.getUsers

    def CreateUser(self, acType=None, username=None, password=None, email=None, groups=None):
        if type(groups) is not list:
            groups = [groups]

        if acType == 'KERBEROS':
            password = 'None'

        if acType and username and groups and password and not email:
            payload = {
                "type": "%s" % acType,
                "username": "%s" % username,
                "password": "%s" % password,
                "groups": groups
            }
        elif acType and username and password and groups and email:
            payload = {
                "type": "%s" % acType,
                "username": "%s" % username,
                "email": "%s" % email,
                "password": "%s" % password,
                "groups": groups
            }
        else:
            return "Missing parameters: acType=, username=, email=, groups="

        if acType == 'KERBEROS':
            payload.pop('password')

        __RQE = self.url + "/api/v1/user"
        self.createUser = exec_request(
            __METHOD="post",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __DATA=payload,
            __VERIFY=self.verify_cert
        )

        return self.createUser

    def UpdateUser(self, acType=None, username=None, password=None, email=None, groups=None):
        if type(groups) is not list:
            groups = [groups]

        if acType == 'KERBEROS':
            password = 'None'

        if acType and username and groups and password and not email:
            payload = {
                "type": "%s" % acType,
                "username": "%s" % username,
                "password": "%s" % password,
                "groups": groups
            }
        elif acType and username and password and groups and email:
            payload = {
                "type": "%s" % acType,
                "username": "%s" % username,
                "email": "%s" % email,
                "password": "%s" % password,
                "groups": groups
            }
        else:
            return "Missing parameters: acType=, username=, email=, groups="

        if acType == 'KERBEROS':
            payload.pop('password')

        __RQE = self.url + "/api/v1/user/" + username
        self.updateUser = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __DATA=payload,
            __VERIFY=self.verify_cert
        )

        return self.updateUser

    def UpdateUserPassword(self, username, password):
        payload = {
            "value": "%s" % password
        }

        __RQE = self.url + "/api/v1/user/" + username + '/password'
        self.updateUserPassword = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __DATA=payload,
            __VERIFY=self.verify_cert
        )

        return self.updateUserPassword

    def DeleteUser(self, username):
        __RQE = self.url + "/api/v1/user/" + username
        self.deleteUser = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.admin_text_plain_headers,
            __VERIFY=self.verify_cert
        )

        return self.deleteUser

    def GetServiceAccounts(self):
        __RQE = self.admin_serviceaccounts_endpoint
        self.getServiceAccounts = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __VERIFY=self.verify_cert
        )

        return self.getServiceAccounts

    def CreateSA(self, name=None, owner=None, groups=None, token=None):
        if type(groups) is not list:
            groups = [groups]

        if name and groups and owner and not token:
            payload = {
                "name": "%s" % name,
                "groups": groups,
                "owner": "%s" % owner
            }
        elif name and groups and owner and token:
            payload = {
                "name": "%s" % name,
                "groups": groups,
                "owner": "%s" % owner,
                "token": "%s" % token
            }
        else:
            return "Missing parameters: name=, groups=, owner=, token="

        __RQE = self.url + "/api/v1/serviceaccount"
        self.createSA = exec_request(
            __METHOD="post",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __DATA=payload,
            __VERIFY=self.verify_cert
        )

        return self.createSA

    def UpdateSA(self, name=None, owner=None, groups=None):
        if type(groups) is not list:
            groups = [groups]

        if name and groups and owner:
            payload = {
                "name": "%s" % name,
                "groups": groups,
                "owner": "%s" % owner
            }
        elif name and groups and owner:
            payload = {
                "name": "%s" % name,
                "groups": groups,
                "owner": "%s" % owner,
            }
        else:
            return "Missing parameters: name=, groups=, owner="

        __RQE = self.url + "/api/v1/serviceaccount/" + name
        self.updateSA = exec_request(
            __METHOD="put",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __DATA=payload,
            __VERIFY=self.verify_cert
        )

        return self.updateSA

    def UpdateSAToken(self, name=None, token=None):
        payload = {
            "token": "%s" % token,
        }

        if token:
            __RQE = self.url + "/api/v1/serviceaccount/" + name + '/revoke'
            self.updateSAToken = exec_request(
                __METHOD="put",
                __EXPECTED="json",
                __URL=__RQE,
                __HEADERS=self.admin_x_headers,
                __DATA=payload,
                __VERIFY=self.verify_cert
            )
        else:
            __RQE = self.url + "/api/v1/serviceaccount/" + name + '/revoke'
            self.updateSAToken = exec_request(
                __METHOD="put",
                __EXPECTED="json",
                __URL=__RQE,
                __HEADERS=self.admin_x_headers,
                __DATA='{}',
                __DT='data',
                __VERIFY=self.verify_cert
            )

        return self.updateSAToken

    def DeleteSA(self, name=None):
        __RQE = self.url + "/api/v1/serviceaccount/" + name
        self.deleteSA = exec_request(
            __METHOD="delete",
            __EXPECTED="text",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __VERIFY=self.verify_cert
        )

        return self.deleteSA

    def GetConfig(self):
        self.getConfig = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=self.admin_config_info_endpoint,
            __HEADERS=self.admin_text_plain_headers,
            __VERIFY=self.verify_cert
        )

        return self.getConfig

    def Audits(self, pageSize=999999999):
        __RQE = self.admin_audits_endpoint
        __RQE = __RQE + '?pageSize=%s' % pageSize
        self.audits = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __VERIFY=self.verify_cert
        )

        if type(self.audits) is dict:
            return self.audits['values']
        else:
            return self.audits

    def Alerts(self, pageSize=999999999):
        __RQE = self.admin_alerts_endpoint
        __RQE = __RQE + '?pageSize=%s' % pageSize
        self.alerts = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __VERIFY=self.verify_cert
        )

        if type(self.alerts) is dict:
            return self.alerts['values']
        else:
            return self.alerts

    def GetLogs(self, type='INFO'):
        __RQE = self.admin_logs_endpoint
        if type.upper() in ['INFO', "METRICS"]:
            __RQE = __RQE + "/%s" % type.upper()
        else:
            return "Wrong GetLogs type. Please give either one of INFO or METRICS"

        self.getLogs = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=__RQE,
            __HEADERS=self.admin_x_headers,
            __VERIFY=self.verify_cert
        )

        return self.getLogs
