from requests import get
from lenses_python.constants import POLICY_END_POINT

class Policy:
    def __init__(self, token, url):
        """

        :param token:
        """
        self.token = token
        self.url = url
        self.url_extend = POLICY_END_POINT
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
            response.raise_for_status()
        return response.json()








