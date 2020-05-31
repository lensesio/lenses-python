#!/usr/bin/env python3

from requests import post, get
from json import dumps
from lensesio.core.exception import lenses_exception


class Basic(lenses_exception):
    def __init__(self, url, username=None, password=None, service_account=None, verify_cert=True):
        self.username = username
        self.password = password
        self.verify_cert = verify_cert

        self.payload = {
            'user': username,
            'password': password
        }

        self.service_account = service_account

        self.default_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/plain'
        }

        self.login_url = url + "/api/login"
        self.auth_url = url + "/api/auth"

    def connect(self):
        if None not in (
            self.payload['user'],
            self.payload['password']
        ):

            self.response = post(
                self.login_url,
                data=dumps(self.payload),
                headers=self.default_headers,
                verify=self.verify_cert
            )

            self.token = self.response.text
        else:
            self.token = 'INVALID'

        self.active_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/plain',
            'X-Kafka-Lenses-Token': self.token
        }

    def serviceConnect(self):
        self.token = self.service_account

        self.active_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/plain',
            'X-Kafka-Lenses-Token': self.token
        }

    def UserInfo(self):
        response = get(
            self.auth_url,
            headers=self.active_headers,
            verify=self.verify_cert
        )

        if response.status_code in [200, 201, 202, 203]:
            self.userInfo = response.json()
        else:
            self.userInfo = response.text

        return self.userInfo

    def ConnectionValidation(self):
        response = self.UserInfo()
        if 'token' in response:
            return 0
        else:
            return 1
