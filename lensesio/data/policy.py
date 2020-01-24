from lensesio.core.endpoints import getEndpoints
from lensesio.core.exec_action import exec_request


class Policy:
    def __init__(self):
        getEndpoints.__init__(self, "policyEndpoints")

        self.lenses_policies_endpoint = self.url + self.lensesPoliciesEndpoint
        self.policy_headers = {
            'Content-Type': 'application/json',
            'Accept': 'text/plain application/json',
            'x-kafka-lenses-token': self.token}

    def ViewPolicy(self):
        self.viewPolicy = exec_request(
            __METHOD="get",
            __EXPECTED="json",
            __URL=self.lenses_policies_endpoint,
            __HEADERS=self.policy_headers
        )

        return self.viewPolicy

    def SetPolicy(self, name, obfuscation, impactType, category, fields):
        if type(fields) is not list:
            fields = [fields]

        params = dict(
            name=name,
            obfuscation=obfuscation,
            impactType=impactType,
            category=category,
            fields=fields
        )
        self.setPolicy = exec_request(
            __METHOD="post",
            __EXPECTED="text",
            __URL=self.lenses_policies_endpoint,
            __HEADERS=self.policy_headers,
            __DATA=params
        )

        return self.setPolicy

    def DelPolicy(self, name):
        policies = self.ViewPolicy()
        for e in policies:
            if e['name'] == name:
                policy_id = e['id']
                break
        else:
            policy_id = None

        if policy_id:
            _REQ = self.lenses_policies_endpoint + '/' + policy_id
            self.delPolicy = exec_request(
                __METHOD="delete",
                __EXPECTED="text",
                __URL=_REQ,
                __HEADERS=self.policy_headers
            )
        else:
            return "No policy with name %s" % name

        return self.delPolicy
