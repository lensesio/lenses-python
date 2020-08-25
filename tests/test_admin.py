import time

class TestAdmin:

    def test_get_groups(self, lenses_conn):
        assert type(lenses_conn.GetGroups()) is list

    def test_create_group(self, lenses_conn):
        group_payload = {
            "name":"test_group",
            "description":"test_description",
            "scopedPermissions":[
                "ViewKafkaConsumers",
                "ManageKafkaConsumers",
                "ViewConnectors",
                "ManageConnectors",
                "ViewSQLProcessors",
                "ManageSQLProcessors",
                "ViewSchemaRegistry",
                "ManageSchemaRegistry",
                "ViewTopology",
                "ManageTopology"
            ],
            "adminPermissions":[
                "ViewDataPolicies",
                "ManageDataPolicies",
                "ViewAuditLogs",
                "ViewUsers",
                "ManageUsers",
                "ViewAlertRules",
                "ManageAlertRules",
                "ViewKafkaSettings",
                "ManageKafkaSettings",
                "ViewLogs"
            ],
            "namespaces":[
                {
                    "wildcards":["*"],
                    "permissions":[
                        "CreateTopic",
                        "DropTopic",
                        "ConfigureTopic",
                        "QueryTopic",
                        "ShowTopic",
                        "ViewSchema",
                        "InsertData",
                        "DeleteData",
                        "UpdateSchema"
                    ],"system":"Kafka","instance":"Dev"
                }
            ]
        }

        assert lenses_conn.CreateGroup(group_payload) == 'OK'

    def test_get_group(self, lenses_conn):
        groups = lenses_conn.GetGroups()
        for g in groups:
            if g['name'] == 'test_group':
                result = True
                break
        else:
            result = False
        
        assert result

    def test_update_group(self, lenses_conn):
        group_payload = {
            "name":"test_group","description":"test_description_updated","namespaces":[
                {
                    "wildcards":["*"],
                    "permissions":[
                        "CreateTopic","DropTopic","ConfigureTopic","QueryTopic","ShowTopic",
                        "ViewSchema","InsertData","DeleteData","UpdateSchema"
                    ],
                    "system":"Kafka","instance":"Dev"
                }
            ],
            "scopedPermissions":[
                "ViewKafkaConsumers","ManageKafkaConsumers","ViewConnectors","ManageConnectors",
                "ViewSQLProcessors","ManageSQLProcessors","ViewSchemaRegistry","ManageSchemaRegistry",
                "ViewTopology","ManageTopology"
            ],
            "adminPermissions":[
                "ViewDataPolicies","ManageDataPolicies","ViewAuditLogs","ViewUsers","ManageUsers",
                "ViewAlertRules","ManageAlertRules","ViewKafkaSettings","ManageKafkaSettings",
                "ViewLogs"
            ],
            "userAccounts":0,"serviceAccounts":0
        }

        response = lenses_conn.UpdateGroup("test_group", group_payload)
        groups = lenses_conn.GetGroups()
        for g in groups:
            if g['name'] == 'test_group':
                if g['description'] == 'test_description_updated':
                    result = True
                    break
                else:
                    result = False
                    break
        else:
            result = False
        assert result and response == 'OK'

    def test_delete_group(self, lenses_conn):
        group_resp = lenses_conn.DeleteGroup("test_group")
        groups = lenses_conn.GetGroups()
        for g in groups:
            if g['name'] == 'test_group':
                result = False
                break
        else:
            result = True
        
        assert result and group_resp == 'OK'

    def test_get_users(self, lenses_conn):
        assert type(lenses_conn.GetUsers()) is list

    def test_create_user_basic_no_email(self, lenses_conn):
        group_payload = {
            "name":"test_group_user",
            "description":"test_description",
            "scopedPermissions":[
                "ViewKafkaConsumers","ManageKafkaConsumers","ViewConnectors","ManageConnectors",
                "ViewSQLProcessors","ManageSQLProcessors","ViewSchemaRegistry","ManageSchemaRegistry",
                "ViewTopology","ManageTopology"
            ],
            "adminPermissions":[
                "ViewDataPolicies","ManageDataPolicies","ViewAuditLogs","ViewUsers","ManageUsers",
                "ViewAlertRules","ManageAlertRules","ViewKafkaSettings","ManageKafkaSettings","ViewLogs"
            ],
            "namespaces":[
                {
                    "wildcards":["*"],
                    "permissions":[
                        "CreateTopic","DropTopic","ConfigureTopic","QueryTopic","ShowTopic",
                        "ViewSchema","InsertData","DeleteData","UpdateSchema"
                    ],"system":"Kafka","instance":"Dev"
                }
            ]
        }

        assert lenses_conn.CreateGroup(group_payload) == 'OK'
        assert lenses_conn.CreateUser(
            acType="BASIC", 
            username="test_user", 
            password="testuser", 
            email=None, 
            groups="test_group_user"
        ) == 'OK'

    def test_create_user_basic(self, lenses_conn):
        assert lenses_conn.CreateUser(
            acType="BASIC", 
            username="test_user_mail", 
            password="testuser", 
            email="test@localhost.localdomain", 
            groups="test_group_user"
        ) == 'OK'

    def test_create_user_kerberos(self, lenses_conn):
        assert lenses_conn.CreateUser(
            acType="KERBEROS", 
            username="test_user_krb", 
            email="test@localhost.localdomain", 
            groups="test_group_user"
        ) == 'OK'

    def test_update_user_basic(self, lenses_conn):
        assert lenses_conn.UpdateUser(
            acType="BASIC", 
            username="test_user_mail", 
            password="testuser", 
            email="test_updated@localhost.localdomain", 
            groups="test_group_user"
        ) == 'OK'

    def test_update_user_kerberos(self, lenses_conn):
        assert lenses_conn.UpdateUser(
            acType="KERBEROS", 
            username="test_user_krb", 
            email="test_updated@localhost.localdomain", 
            groups="test_group_user"
        ) == 'OK'

    def test_update_user_password(self, lenses_conn):
        assert lenses_conn.UpdateUserPassword(
            username="test_user_mail",
            password="testpwd"
        ) == 'OK'
    
    def test_delete_user_basic_no_mail(self, lenses_conn):
        assert lenses_conn.DeleteUser(
            username="test_user",
        ) == 'OK'

    def test_delete_user_basic(self, lenses_conn):
        assert lenses_conn.DeleteUser(
            username="test_user_mail",
        ) == 'OK'

    def test_delete_user_krb(self, lenses_conn):
        assert lenses_conn.DeleteUser(
            username="test_user_krb",
        ) == 'OK'

    def test_delete_usergroup(self, lenses_conn):
        assert lenses_conn.DeleteGroup("test_group_user") == 'OK'

    def test_create_service_account(self, lenses_conn):
        group_payload = {
            "name":"test_group_sa",
            "description":"test_description",
            "scopedPermissions":[
                "ViewKafkaConsumers","ManageKafkaConsumers","ViewConnectors","ManageConnectors",
                "ViewSQLProcessors","ManageSQLProcessors","ViewSchemaRegistry","ManageSchemaRegistry",
                "ViewTopology","ManageTopology"
            ],
            "adminPermissions":[
                "ViewDataPolicies","ManageDataPolicies","ViewAuditLogs","ViewUsers","ManageUsers",
                "ViewAlertRules","ManageAlertRules","ViewKafkaSettings","ManageKafkaSettings","ViewLogs"
            ],
            "namespaces":[
                {
                    "wildcards":["*"],
                    "permissions":[
                        "CreateTopic","DropTopic","ConfigureTopic","QueryTopic","ShowTopic",
                        "ViewSchema","InsertData","DeleteData","UpdateSchema"
                    ],"system":"Kafka","instance":"Dev"
                }
            ]
        }

        assert lenses_conn.CreateGroup(group_payload) == 'OK'
        assert lenses_conn.CreateUser(
            acType="BASIC", 
            username="test_user_sa", 
            password="testpwd", 
            email="test_user_sa@localhost.localdomain", 
            groups="test_group_sa"
        ) == 'OK'
        assert lenses_conn.CreateSA(
            name="test_sa",
            groups="test_group_sa",
            owner="test_user_sa",
            token="test_sa_token"
        )['token'] == 'test_sa_token'

    def test_update_service_account(self, lenses_conn):
        group_payload = {
            "name":"test_group_sa2",
            "description":"test_description",
            "scopedPermissions":[
                "ViewKafkaConsumers","ManageKafkaConsumers","ViewConnectors","ManageConnectors",
                "ViewSQLProcessors","ManageSQLProcessors","ViewSchemaRegistry","ManageSchemaRegistry",
                "ViewTopology","ManageTopology"
            ],
            "adminPermissions":[
                "ViewDataPolicies","ManageDataPolicies","ViewAuditLogs","ViewUsers","ManageUsers",
                "ViewAlertRules","ManageAlertRules","ViewKafkaSettings","ManageKafkaSettings","ViewLogs"
            ],
            "namespaces":[
                {
                    "wildcards":["*"],
                    "permissions":[
                        "CreateTopic","DropTopic","ConfigureTopic","QueryTopic","ShowTopic",
                        "ViewSchema","InsertData","DeleteData","UpdateSchema"
                    ],"system":"Kafka","instance":"Dev"
                }
            ]
        }

        assert lenses_conn.CreateGroup(group_payload) == 'OK'
        assert lenses_conn.UpdateSA(
            name="test_sa",
            groups=["test_group_sa", "test_group_sa2"],
            owner="test_user_sa",
        ) == 'OK'

    def test_update_service_account_token_fixed(self, lenses_conn):
        assert lenses_conn.UpdateSAToken(
            name="test_sa",
            token="test_sa_token_updated",
        )['token'] == 'test_sa_token_updated'

    def test_update_service_account_token_random(self, lenses_conn):
        assert 'token' in lenses_conn.UpdateSAToken(name="test_sa",).keys() \
        and len(lenses_conn.UpdateSAToken(name="test_sa",)['token']) >= 36

    def test_delete_service_account(self, lenses_conn):
        assert lenses_conn.DeleteSA(name="test_sa") == 'OK'
        time.sleep(5)
        assert lenses_conn.DeleteUser(
            username="test_user_sa",
        ) == 'OK'
        assert lenses_conn.DeleteGroup("test_group_sa") == 'OK'
        assert lenses_conn.DeleteGroup("test_group_sa2") == 'OK'

    def test_get_config(self, lenses_conn):
        assert 'lenses.version' in lenses_conn.GetConfig().keys()

    def test_get_audits(self, lenses_conn):
        assert type(lenses_conn.Audits())

    def test_get_alerts(self, lenses_conn):
        assert type(lenses_conn.Alerts())

    def test_get_logs(self, lenses_conn):
        assert type(lenses_conn.GetLogs())