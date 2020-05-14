#!/usr/bin/env python3

from lensesio.core.exception import lenses_exception
from lensesio.core.admin import AdminPanel
from lensesio.core.basic_auth import Basic
from lensesio.kafka.topics import KafkaTopic
from lensesio.registry.schemas import SchemaRegistry
from lensesio.data.sql import SQLExec
from lensesio.kafka.quotas import KafkaQuotas
from lensesio.data.policy import Policy
from lensesio.data.processors import DataProcessor
from lensesio.data.connectors import DataConnector
from lensesio.kafka.acls import KafkaACL
from lensesio.data.data_subscribe import DataSubscribe
from lensesio.data.consumers import DataConsumers
from lensesio.data.topology import Topology
from lensesio.flows.flows import LensesFlows
from lensesio.pulsar.pulsar_producer import SetupPulsar
from sys import exit
import platform


class main(
            Basic, KafkaTopic, SchemaRegistry, SQLExec,
            KafkaQuotas, Policy, DataProcessor, DataConnector,
            KafkaACL, DataSubscribe, LensesFlows, lenses_exception,
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
            verify_cert=False):
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
            if platform.system().lower() in ['linux', 'linux2', 'darwin']:
                from lensesio.core.krb_auth import krb5
                self.krb5 = krb5
                self.krb5.__init__(self, url=url, service=krb_service)
                self.krb5.KrbAuth(self)
            else:
                msg = "Error: gssapi kerberos integration is not supported for "
                print(msg + platform.system())
                exit(1)

        if self.ConnectionValidation() == 1:
            print("Could not login to lenses. Please check the auth options")
            exit(1)

        AdminPanel.__init__(self)
        Topology.__init__(self)
        KafkaTopic.__init__(self)
        SchemaRegistry.__init__(self)
        SQLExec.__init__(self)
        KafkaQuotas.__init__(self)
        Policy.__init__(self)
        DataProcessor.__init__(self)
        DataConnector.__init__(self)
        KafkaACL.__init__(self)
        DataSubscribe.__init__(self)
        DataConsumers.__init__(self)
    
    def InitPulsar(self, host):
        SetupPulsar.__init__(self, host)