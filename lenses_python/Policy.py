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








