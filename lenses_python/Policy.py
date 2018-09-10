from requests import *

class Policy:
    def __init__(self, token, url):
        """

        :param token:
        """
        self.token = token
        self.url = url
        self.url_extend = "/api/protection/policy"
        self.default_headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                                'x-kafka-lenses-token': self.token}

    def update_policy(self, policy_id, policy):
        """
        Updata policy

        :param policy_id:The unique identifier for the policy Example: "0".String
        :param policy:{
                         "id": "18",
                         "name": "College Card ID",
                         "category": "PII",
                         "impact": "HIGH",
                        "obfuscation": "First-2",
                        "fields": "college_card_id, college_id_num"
                     }
        :return:
        """
        url = self.url+"/"+self.url_extend+policy_id
        response = put(url, data=policy, headeres=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def delete_policy(self, policy_id):
        """
        Delete policy

        :param policy_id: The unique identifier for the policy Example: 0.Number
        :return:
        """
        url = self.url+"/"+self.url_extend+policy_id
        response = put(url, headeres=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()

    def view_policy(self):
        """
        View all versions for a policy

        :return:
        """
        url = self.url+self.url_extend
        response = get(url, headers=self.default_headers)
        if response.status_code != 200:
            raise Exception("Http status code {}.{}".format(response.status_code, response.text))
        return response.json()








