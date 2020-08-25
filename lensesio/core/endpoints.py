class lensesEndpoints():

    def adminEndpoint(self):
        self.lensesAdminAuditsEndpoint = "/api/audit"
        self.lensesAdminAlertsEndpoint = "/api/alerts"
        self.lensesConfigInfoEndpoint = "/api/config"
        self.lensesLogsEndpoint = "/api/logs"
        self.lensesGroupsEndpoint = "/api/v1/group"
        self.lensesUsersEndpoint = "/api/v1/user"
        self.lensesServiceAccountEndpoint = "/api/v1/serviceaccount"

    def aclEndpoints(self):
        # KafkaACL
        self.lensesAclEndpoint = "/api/acl"

    def connectEndpoints(self):
        # DataConnector
        self.lensesConnectEndpoint = "/api/proxy-connect"

    def consumersEndpoints(self):
        # DataConsumer
        self.lensesConsumersEndpoint = "/api/consumers"

    def topologyEndpoints(self):
        # Topology
        self.lensesTopologyEndpoint = "/api/topology"

    def policyEndpoints(self):
        # Policy
        self.lensesPoliciesEndpoint = "/api/protection/policy"

    def processorEndpoints(self):
        # DataProcessor
        self.lensesProcessorsEndpoint = "/api/v1/streams"

    def websocketEndpoints(self):
        # DataWebsockets
        self.lensesWebsocketEndpoint = "/api/kafka/ws/"

    def quotaEndpoints(self):
        # KafkaQuota
        self.lensesQuotasEndpoint = "/api/quotas/"
        self.lensesUserQuotasEndpoint = "/api/quotas/users"
        self.lensesClientQuotasEndpoint = "/api/quotas/clients"

    def schemaEndpoints(self):
        # SchemaRegistry
        self.lensesSchemasEndpoint = "/api/proxy-sr/subjects"
        self.lensesSchemasIDsEndpoint = "/api/proxy-sr/schemas/ids"
        self.lensesSchemasConfigEndpoint = "/api/proxy-sr/config"

    def topicEndpoints(self):
        # KafkaTopics
        self.lensesTopicsEndpoint = "/api/v1/kafka/topics"
        self.lensesTopicsConfigEndpoint = "/api/configs/default/topics"
        self.lensesManageTopicEndpoint = "/api/topics"
        self.lensesTopicConfigUpdate = "/api/configs/topics"

    def sqlEndpoints(self):
        # SQL
        self.lensesSQLValidationEndpoint = '/api/v1/sql/presentation'
        self.lensesSQLEndpoint = "/api/ws/v2/sql/execute?"


class getEndpoints():
    def __init__(self, _method):
        getattr(lensesEndpoints, _method)(self)
