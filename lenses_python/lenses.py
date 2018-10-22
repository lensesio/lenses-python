from requests import post, get
from json import dumps

class lenses:

    def __init__(self, url, username="", password="", kerberos_mode=0):
        """
        :param url:
        :param username:
        :param password:
        :param kerberos_mode: if it's equal to 1 we have kerberos mode, in default the value is 0 so no kerberos_mode
        """

        self.url = url
        self.username = username
        self.password = password
        self.len_mods()

        # Check if user use kerberos mode
        if kerberos_mode == 0:
            # If not get the token with basic authentication
            self._Connect()
            self.default_headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                                    'x-kafka-lenses-token': self.token}
        else:
            # else take the token with kerberos connect way
            self._KerberosConnect()

    # CHECK THE LENSES PYMODS
    def len_mods(self):
        _check="[\033[5;32m * \033[0;0m]"
        print(_check + " Checking lenses pylib modules.")

        try:
            from lenses_python.SqlHandler import SqlHandler as SqlH
            from lenses_python.TopicHandler import TopicHandler as TopicH
            from lenses_python.ProcessorHandler import ProcessorHandler as PrcH
            from lenses_python.SchemasHandler import SchemasHandler as SchemaH
            from lenses_python.ConnectorHandler import ConnectorHandler as ConnH
            from lenses_python.WebSocketHandler import SubscribeHandler as SubSH
            from lenses_python.PublishHandler import PublishHandler as PuHandl
            from lenses_python.ACLHandler import ACLHandler
            from lenses_python.QuotaHandler import QuotaHandler
            from lenses_python.KerberosTicket import KerberosTicket

        except ImportError:
            raise
            exit(1)

        print(_check + " No issue detected.\n")
        del _check
        #global SqlH, TopicH, PrcH, SchemaH, ConnH, SubSH, PuHandl, ACLHandler, QuotaHandler, KerberosTicket
        del SqlH, TopicH, PrcH, SchemaH, ConnH, SubSH, PuHandl, ACLHandler, QuotaHandler, KerberosTicket

    def kopts(self, action, *opts, **kopts):

        optional = ['config', 'filename', 'partitions', 'replication']

        # ManageTopics
        optlist = {"ManageTopics":{'create':['topicname', 'replication', 'partitions', 'config', 'filename']}}
        optlist["ManageTopics"]['get'], optlist["ManageTopics"]['list'] = [], []
        optlist["ManageTopics"]['info'], optlist["ManageTopics"]['delete'] = ['topicname'], ['topicname']
        optlist["ManageTopics"]['update'] = ['topicname', 'config', 'filename']
        optlist["ManageTopics"]['delrec'] = ['topicname', 'partition', 'offset']
        optlist["ManageTopics"]['config'] = ['topicname', 'config', 'filename']

        # ManageProcessor
        optlist["ManageProcessor"] = {'create':['name', 'sql', 'runners', 'clustername', 'namespace', 'pipeline']}
        optlist["ManageProcessor"]['updrun'] = ['processorname', 'runesnumber']

        # ManageSR
        optlist["ManageSR"] = {'getbyver':['subject', 'verid']}
        optlist["ManageSR"]['regnew'] = ['subject', 'schema_json', 'filename']
        optlist["ManageSR"]['delver'] = ['subject', 'verid']
        optlist["ManageSR"]['chngcomp'] = ['subject', 'compability', 'filename']
        optlist["ManageSR"]['updglobcomp'] = ['compability', 'filename']

        # ManageConnector
        optlist["ManageConnector"] = {'create':['cluster', 'config', 'filename']}
        optlist["ManageConnector"]['info'] = ['cluster', 'connector']
        optlist["ManageConnector"]['getconfig'] = ['cluster', 'connector']
        optlist["ManageConnector"]['status'] = ['cluster', 'connector']
        optlist["ManageConnector"]['gettask'] = ['cluster', 'connector']
        optlist["ManageConnector"]['taskstatus'] = ['cluster', 'connector']
        optlist["ManageConnector"]['taskrestart'] = ['cluster', 'connector', 'task_id']
        optlist["ManageConnector"]['pause'] = ['cluster', 'connector']
        optlist["ManageConnector"]['resume'] = ['cluster', 'connector']
        optlist["ManageConnector"]['restart'] = ['cluster', 'connector']
        optlist["ManageConnector"]['setconfig'] = ['cluster', 'connector', 'config', 'filename']
        optlist["ManageConnector"]['delete'] = ['cluster', 'connector']

        # ManageWS
        optlist["ManageWS"] = {'subschandler':['clientId', 'url_ws', 'query', 'write', 'filename', 'print_results', 'datetimelist', 'formatinglist']}
        optlist["ManageWS"]['publish'] = ['subject', 'compability', 'filename']
        optlist["ManageWS"]['commit'] = ['subject', 'compability', 'filename']
        optlist["ManageWS"]['unsubscribe'] = ['compability', 'filename']
        optlist["ManageWS"]['subscribe'] = ['compability', 'filename']

        # ManageACL
        optlist["ManageACL"] = {'setacl':['resourceType', 'resourceName', 'principal', 'permissionType', 'host', 'operation']}

        # ManageQuotas
        optlist["ManageQuotas"] = {'getquotas':[]}
        optlist["ManageQuotas"]['setqallusers'] = ['config']
        optlist["ManageQuotas"]['setquallcli'] = ['user', 'config']
        optlist["ManageQuotas"]['setqucli'] = ['user', 'clientid', 'config']
        optlist["ManageQuotas"]['setqu'] = ['user', 'config']
        optlist["ManageQuotas"]['setqallcli'] = ['config']
        optlist["ManageQuotas"]['setqcli'] = ['clientid', 'config']
        optlist["ManageQuotas"]['dellqallu'] = ['config']
        optlist["ManageQuotas"]['delquallcli'] = ['user', 'config']
        optlist["ManageQuotas"]['delqucli'] = ['user', 'clientid', 'config']
        optlist["ManageQuotas"]['delqu'] = ['user', 'config']
        optlist["ManageQuotas"]['delqallcli'] = ['config']
        optlist["ManageQuotas"]['delqcli'] = ['clientid', 'config']

    def _Connect(self):
        """

        :return:Token
        """
        login_url = self.url + "/api/login"

        bodystring = {'user': self.username,
                   'password': self.password}

        default_headers = {'Content-Type': 'text/plain',
                           'Accept': 'text/plain'}

        response = post(login_url, data=dumps(bodystring), headers=default_headers)
        if response.status_code != 200:
            raise Exception("Could not connect to the API [{}]. Status code [{}]. Reason [{}]"
                            .format(login_url, response.status_code, response.reason))
        else:
            # self.token = response.json().get("token", None)
            self.token = response.text
            if self.token == None:
                raise Exception("Cannot recieve Token.")

        # auth_url = self.url + "/api/auth"
        # new_headers = {"X-Kafka-Lenses-Token": self.token
        #                }
        #
        # response = get(auth_url, headers=new_headers)
        # if response.status_code != 200:
        #     raise Exception("Could not connect to the API [{}]. Status code [{}]. Reason [{}]"
        #                     .format(auth_url, response.status_code, response.reason))
        # else:
        #     self.token = response.json().get("token", None)
        #     self.credentials = response.json()

    def _KerberosConnect(self):
        """
        Use this function to take Kerberos ticket and then make get request to take.
        The input of KerberosTicket, the service, is the url, without protocol(https:// for example)
        :return:
        """
        service = self.url.split('//')[-1]
        ticket = KerberosTicket("HTTP@"+service).auth_header
        AUTH = "/api/auth"
        auth_url = self.url + AUTH
        kerberos_headers = {
                             "Authorization": ticket
                            }
        response = get(auth_url, headers=kerberos_headers)
        if response.status_code != 200:
            raise Exception("Could not connect to the API [{}]. Status code [{}]. Reason [{}]"
                            .format(auth_url, response.status_code, response.reason))
        else:
            self.token = response.json().get("token", None)
            self.credentials = response.json()

    # Sql Handler
    def SqlHandler(self, query, extract_pandas=0, datetimelist=[], formatinglist=[]):
        """

        :param query:
        :param extract_pandas: if is 0 return dict else return pandas dataframes
        :param datetimelist: List of keys which content datetime string
        :param formatinglist: List of formation of elements of datetimelist keys
        :return:The result of the given query
        """
        return SqlH(self.url, self.username, self.password, self.token, query, datetimelist, formatinglist).ExecuteSqlQuery(extract_pandas)

    # Topics Handler
    def ManageTopics(self, action, *opts, **kopts):
        from lenses_python.TopicHandler import TopicHandler as TopicH
        action_dict = {'get':'GetAllTopics', 'list':'LstOfTopicsNames', 'info':'TopicInfo', 'delete':'DeleteTopic', \
        'config':'UpdateTopicConfig', 'create':'CreateTopic', 'delrec':'DeleteTopicRecords' }

        self.opts = {_opt:kopts.get(_opt, '') for _opt in kopts}
        _exec_action = getattr(TopicH(self), action_dict[action])(**self.opts)

        return _exec_action

    # Processor Handler
    def ManageProcessor(self, action, *opts, **kopts):
        from lenses_python.ProcessorHandler import ProcessorHandler as PrcH
        action_dict = {'create':'CreateProcessor', 'delete':'DeleteProcessor', 'resume':'ResumeProcessor', \
        'pause':'PauseProcessor', 'updrun':'UpdateProcessorRunners' }

        self.opts = {_opt:kopts.get(_opt, '') for _opt in kopts}
        _exec_action = getattr(PrcH(self), action_dict[action])(**self.opts)

        return _exec_action

    # Schemas Handler
    def ManageSR(self, action, *opts, **kopts):
        from lenses_python.SchemasHandler import SchemasHandler as SchemaH
        action_dict = {'listsubj':'ListAllSubjects', 'getglob':'GetGlobalCompatibility', 'subjver':'ListVersionsSubj', \
        'getsrid':'GetSchemaById', 'getcomp':'GetCompatibility', 'delete':'DeleteSubj', 'getbyver':'GetSchemaByVer', \
        'regnew':'RegisterNewSchema', 'delver':'DeleteSchemaByVersion', 'chngcomp':'ChangeCompatibility', \
        'updglobcomp': 'UpdateGlobalCompatibility'}

        self.opts = {_opt:kopts.get(_opt, '') for _opt in kopts}
        _exec_action = getattr(SchemaH(self), action_dict[action])(self.opts)

        return _exec_action

    # Connector Handler
    def ManageConnector(self, action, *opts, **kopts):
        from lenses_python.ConnectorHandler import ConnectorHandler as ConnH
        action_dict = {'listall':'ListAllConnectors', 'getplugins':'GetConnectorPlugins', 'info':'GetInfoConnector', \
        'getconfig':'GetConnectorConfig', 'status':'GetConnectorStatus', 'gettask':'GetConnectorTasks', \
        'pause':'PauseConnector', 'resume':'ResumeConnector', 'delete':'DeleteConnector', 'restart':'RestartConnector', \
        'taskstatus':'GetStatusTask', 'taskrestart':'RestartConnectorTask', 'create':'CreateConnector', \
        'setconfig': 'SetConnectorConfig'}

        self.opts = {_opt:kopts.get(_opt, '') for _opt in kopts}
        _exec_action = getattr(ConnH(self), action_dict[action])(self.opts)

        return _exec_action

    # WebSocketHandler
    def ManageWE(self, action, *opts, **kopts):
        action_dict = {'subschandler':'SubscribeHandler', 'publish':'Publish', 'commit':'Commit', \
        'unsubscribe':'Unscribe', 'subscribe':'Subscribe'}
        from lenses_python.WebSocketHandler import SubscribeHandler as SubSH
        from lenses_python.PublishHandler import PublishHandler as PuHandl

        self.opts = {_opt:kopts.get(_opt, '') for _opt in kopts}
        for x in ['print_results', 'write']:
            if kopts[x] == 'y':
                self.opts[x]=True
            else:
                self.opts[x]=False

        if action == 'subschandler':
            _exec_action = SubSH(self, self.opts)
        else:
            _exec_action = getattr(PuHandl(self, self.opts['clientId'], self.opts['url_ws']), action_dict[action])(self.opts)

        return _exec_action

    # ACL Handler
    def ManageACL(self, action, *opts, **kopts):
        from lenses_python.ACLHandler import ACLHandler
        action_dict = {'getacl':'GetACL', 'setacl':'SetAcl'}

        self.opts = {_opt:kopts.get(_opt, '') for _opt in kopts}
        _exec_action = getattr(ACLHandler(self), action_dict[action])(self.opts)

        return _exec_action

    # Quotas Handler
    def ManageQuotas(self, action, *opts, **kopts):
        from lenses_python.ACLHandler import QuotaHandler

        action_dict = {'getquotas':'GetQuotas', 'setqallusers':'SetQuotasAllUsers', 'setquallcli':'SetQuotaUserAllClients', \
        'setqucli':'SetQuotaUserClient', 'setqu':'SetQuotaUser', 'setqallcli':'SetQuotaAllClient', 'setqcli':'SetQuotaClient', \
        'dellqallu':'DeleteQutaAllUsers', 'delquallcli':'DeleteQuotaUserAllClients', 'delqucli':'DeleteQuotaUserClient', \
        'delqu':'DeleteQuotaUser', 'delqallcli':'DeleteQuotaAllClients', 'delqcli':'DeleteQuotaClient'}

        self.opts = {_opt:kopts.get(_opt, '') for _opt in kopts}
        _exec_action = getattr(ACLHandler(self), action_dict[action])(self.opts)

        return _exec_action
