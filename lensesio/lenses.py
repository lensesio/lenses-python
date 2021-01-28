#!/usr/bin/env python3


from lensesio.pulsar.pulsar_client import SetupPulsar
from lensesio.core.exception import lenses_exception
from lensesio.registry.schemas import SchemaRegistry
from lensesio.data.processors import DataProcessor
from lensesio.data.connectors import DataConnector
from lensesio.data.consumers import DataConsumers
from threading import Thread, enumerate, RLock
from lensesio.kafka.quotas import KafkaQuotas
from lensesio.flows.flows import LensesFlows
from lensesio.kafka.topics import KafkaTopic
from lensesio.data.topology import Topology
from lensesio.core.admin import AdminPanel
from lensesio.core.basic_auth import Basic
from lensesio.kafka.acls import KafkaACL
from lensesio.data.policy import Policy
from lensesio.data.sql import SQLExec
from sys import modules as sys_mods
from sys import exit
import platform


ThreadLock = RLock()

active_threads = {
    'sql': {
        "t": 0,
    },
    'subscribe': {
        "t": 0,
    },
    'pulsar_consumer': {
        "t": 0,
    },
    'pulsar_reader': {
        "t": 0,
    },
    "thread_lock": ThreadLock
}


class main(
            Basic, KafkaTopic, SchemaRegistry, SQLExec,
            KafkaQuotas, Policy, DataProcessor, DataConnector,
            KafkaACL, LensesFlows, lenses_exception,
            DataConsumers, Topology, AdminPanel, SetupPulsar,
        ):
    def __init__(
            self,
            auth_type="basic",
            url=None,
            username=None,
            password=None,
            krb_service=None,
            service_account=None,
            verify_cert=True):
        
        if auth_type is None:
            return

        self.active_threads = active_threads

        try:
            if auth_type not in ['basic', 'service', 'krb5']:
                print('''
                Parameters:
                    Mandatory:
                        auth_type=basic/krb5/service
                        url=lenses endpoint
                    Optional:
                        username (
                            if auth_type is basic
                        )
                        password (
                            if username was defined
                        )
                        service_account (
                            if auth_type is basic
                        )
                        krb_service (
                            if auth_type is krb5 and platform
                            is either one of linux, darwin
                        )
                ''')
                exit(1)
        except NameError:
            print("Please provide auth_type [basic, krb5, service]")
            exit(1)

        self.auth_type = auth_type
        self.url = url

        if self.auth_type == 'basic':
            Basic.__init__(self, url=url, username=username, password=password, verify_cert=verify_cert)
            self.connect()
        if self.auth_type == 'service':
            Basic.__init__(self, url=url, service_account=service_account, verify_cert=verify_cert)
            self.serviceConnect()
        elif self.auth_type == 'krb5':
            if platform.system().lower() not in ['linux', 'linux2', 'darwin']:
                msg = "Error: gssapi kerberos integration is not supported for "
                print(msg + platform.system())
                exit(1)

            try:
                from lensesio.core.krb_auth import krb5
                self.krb5 = krb5
                self.krb5.__init__(self, url=url, service=krb_service)
                self.krb5.KrbAuth(self)
            except NameError:
                print("Kerberos client lib is not installed")
                return None

        if self.ConnectionValidation() == 1:
            print("Could not login to lenses. Please check the auth options")
            exit(1)

        AdminPanel.__init__(self, verify_cert=verify_cert)
        Topology.__init__(self, verify_cert=verify_cert)
        KafkaTopic.__init__(self, verify_cert=verify_cert)
        SchemaRegistry.__init__(self, verify_cert=verify_cert)
        SQLExec.__init__(self, active_threads=active_threads, verify_cert=verify_cert)
        KafkaQuotas.__init__(self, verify_cert=verify_cert)
        Policy.__init__(self, verify_cert=verify_cert)
        DataProcessor.__init__(self, verify_cert=verify_cert)
        DataConnector.__init__(self, verify_cert=verify_cert)
        KafkaACL.__init__(self, verify_cert=verify_cert)
        DataConsumers.__init__(self, verify_cert=verify_cert)

    def InitPulsarClient(self, host, **kwargs):
        try:
            self.Pulsar = SetupPulsar.__init__(self, active_threads, host)
        except NameError:
            print("Pulsar client lib is not installed")
            return None
