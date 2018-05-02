from unittest import TestCase
from lenses_python.lenses import lenses


class TestLenses(TestCase):
    # def test__Connect(self):
    #     self.fail()

    def test_GetCredentials(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        recv = {'user': {'name': 'Lenses Admin', 'roles': ['admin', 'write', 'read', 'nodata'],
                         'email': None, 'id': 'admin'}}
        self.assertEqual(conn.GetCredentials()['user'], recv['user'])

    def test_SqlHandler(self):
        recv = {'messages': [{'value': '{"VendorID":2,"tpep_pickup_datetime":"2016-01-01 00:00:00",'
                                       '"tpep_dropoff_datetime":"2016-01-01 00:00:00","passenger_count":2,'
                                       '"trip_distance":1.1,"pickup_longitude":-73.99037170410156,'
                                       '"pickup_latitude":40.73469543457031,"RateCodeID":1,"store_and_fwd_flag":"N",'
                                       '"dropoff_longitude":-73.98184204101562,"dropoff_latitude":40.73240661621094,'
                                       '"payment_type":2,"fare_amount":7.5,"extra":0.5,"mta_tax":0.5,'
                                       '"improvement_surcharge":0.3,"tip_amount":0.0,"tolls_amount":0.0,'
                                       '"total_amount":8.8}', 'key': None, 'timestamp': 1525255998747,
                              'topic': 'nyc_yellow_taxi_trip_data', 'partition': 0, 'offset': 0}], 'offset': []}
        conn = lenses("http://localhost:3030", "admin", "admin")
        query = "SELECT * FROM `nyc_yellow_taxi_trip_data` WHERE _vtype='AVRO' AND _ktype='BYTES' AND _sample=2 " \
                "AND _sampleWindow=200 limit 1"
        self.assertEqual(conn.SqlHandler(query), recv)

    def test_GetAllTopics(self):
        recv = {'topicName': '_kafka_lenses_lsql_storage'}
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetAllTopics()[0]['topicName'], recv['topicName'])

    def test_TopicInfo(self):
        topic_name = '_kafka_lenses_lsql_storage'
        recv = {'topicName': '_kafka_lenses_lsql_storage'}
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.TopicInfo(topic_name)['topicName'] , recv['topicName'])

    def test_TopicsNames(self):
        recv = ['_kafka_lenses_lsql_storage', 'cc_data', '_kafka_lenses_cluster', 'telecom_italia_grid', 'cc_payments',
                'connect-configs', 'fast_vessel_processor', 'reddit_posts', '__consumer_offsets', 'backblaze_smart',
                'telecom_italia_data', '_kafka_lenses_processors', 'nyc_yellow_taxi_trip_data',
                'sea_vessel_position_reports', '_schemas', '_kafka_lenses_audits', '_kafka_lenses_alerts',
                '_kafka_lenses_profiles', 'connect-offsets', 'logs_broker', 'connect-statuses',
                '_kafka_lenses_alerts_settings']
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.TopicsNames(), recv)

    # def test_UpdateTopicConfig(self):
    #     self.fail()

    # def test_CreateTopic(self):
    #     config = {
    #         "cleanup.policy": "compact",
    #          "compression.type":"snappy"
    #          }
    #     conn = lenses("http://localhost:3030", "admin", "admin")
    #     raised = False
    #     try:
    #         conn.CreateTopic("test1", 1, 1, config)
    #     except:
    #         raised = True
    #     assert (raised == False)

    # def test_DeleteTopic(self):
    #     self.fail()
    #
    def test_CreateProcessor(self):
        query = " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and _" \
                "ktype=AVRO and _vtype=AVRO "
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.CreateProcessor("test_processor_1", query, 1, 'dev', 'ns', '1').split('_')[0], 'lsql')


    def test_DeleteProcessor(self):
        query = " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and _" \
                "ktype=AVRO and _vtype=AVRO "
        conn = lenses("http://localhost:3030", "admin", "admin")
        try:
            processor_id = conn.CreateProcessor("test_processor_2", query, 1, 'dev', 'ns', '1')
            conn.DeleteProcessor(processor_id)
        except Exception as e:
            raise AssertionError('Unexcepted raise exception:', e)

    def test_ResumeProcessor(self):
        query = " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and _" \
                "ktype=AVRO and _vtype=AVRO "
        conn = lenses("http://localhost:3030", "admin", "admin")
        try:
            processor_id = conn.CreateProcessor("test_processor_3", query, 1, 'dev', 'ns', '1')
            conn.ResumeProcessor(processor_id)
        except Exception as e:
            raise AssertionError('Unexcepted raise exception:', e)

    def test_PauseProcessor(self):
        query = " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and _" \
                "ktype=AVRO and _vtype=AVRO "
        conn = lenses("http://localhost:3030", "admin", "admin")
        try:
            processor_id = conn.CreateProcessor("test_processor_4", query, 1, 'dev', 'ns', '1')
            conn.PauseConnector(processor_id)
        except Exception as e:
            raise AssertionError('Unexcepted raise exception:', e)

    def test_UpdateProcessorRunners(self):
        query = " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and _" \
                "ktype=AVRO and _vtype=AVRO "
        conn = lenses("http://localhost:3030", "admin", "admin")
        try:
            processor_id = conn.CreateProcessor("test_processor_4", query, 1, 'dev', 'ns', '1')
            conn.UpdateProcessorRunners(processor_id, '4')
        except Exception as e:
            raise AssertionError('Unexcepted raise exception:', e)


    def test_GetAllSubjects(self):
        recv = ['telecom_italia_data-key', 'cc_payments-value', 'reddit_posts-value',
                'sea_vessel_position_reports-value', 'telecom_italia_grid-value', 'fast_vessel_processor-value',
                'reddit_posts-key', 'telecom_italia_grid-key', 'telecom_italia_data-value',
                'nyc_yellow_taxi_trip_data-value', 'sea_vessel_position_reports-key', 'cc_data-value',
                'fast_vessel_processor-key', 'logs_broker-value']
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetAllSubjects(), recv)


    def test_ListVersionsSubj(self):
        subj = 'telecom_italia_data-key'
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.ListVersionsSubj(subj), [1])

    def test_GetSchemaById(self):
        recv = {'schema': '{"type":"record","name":"Key","namespace":'
                          '"com.landoop.telecom.telecomitalia.telecommunications","fields":[{"name":"SquareId",'
                          '"type":"int","doc":" The id of the square that is part of the Milano GRID."}]}'}
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetSchemaById('8'), recv)


    def test_GetSchemaByVer(self):
        subj = 'telecom_italia_data-key'
        recv = {'subject': 'telecom_italia_data-key', 'version': 1, 'id': 8,
                'schema': '{"type":"record","name":"Key","namespace":'
                          '"com.landoop.telecom.telecomitalia.telecommunications",'
                          '"fields":[{"name":"SquareId","type":"int",'
                          '"doc":" The id of the square that is part of the Milano GRID."}]}'}
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetSchemaByVer(subj, '1'), recv)


    def test_RegisterNewSchema(self):
        schema = {'schema': '{"type":"record","name":"reddit_post_key",'
                            '"namespace":"com.landoop.social.reddit.post.key",'
                            '"fields":[{"name":"subreddit_id","type":"string"}]}'
                  }
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(list(conn.RegisterNewSchema("test", schema).keys())[0], 'id')

    def test_GetGlobalCompatibility(self):
        recv = {'compatibilityLevel': 'BACKWARD'}
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetGlobalCompatibility(), recv)

    # def test_GetCompatibility(self):
    #     self.fail()
    #
    # def test_DeleteSubj(self):
    #     self.fail()
    #
    # def test_DeleteSchemaByVersion(self):
    #     self.fail()
    #
    # def test_ChangeCompatibility(self):
    #     self.fail()
    #
    # def test_UpdateGlobalCompatibility(self):
    #     self.fail()

    def test_ListAllConnectors(self):
        recv = ['logs-broker', 'nullsink']
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.ListAllConnectors('dev'), recv)

    def test_GetInfoConnector(self):
        recv = {'name': 'logs-broker', 'tasks': [{'connector': 'logs-broker', 'task': 0}],
                'config': {'file': '/var/log/broker.log', 'name': 'logs-broker', 'topic': 'logs_broker',
                           'connector.class': 'org.apache.kafka.connect.file.FileStreamSourceConnector',
                           'tasks.max': '1'}, 'type': 'source'}
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetInfoConnector('dev', 'logs-broker'), recv)

    def test_GetConnectorConfig(self):
        recv = {'file': '/var/log/broker.log', 'name': 'logs-broker', 'topic': 'logs_broker',
                'connector.class': 'org.apache.kafka.connect.file.FileStreamSourceConnector', 'tasks.max': '1'}
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetConnectorConfig('dev', 'logs-broker'), recv)

    def test_GetConnectorStatus(self):
        recv = {'name': 'logs-broker', 'tasks': [{'state': 'RUNNING', 'worker_id': '172.17.0.2:8083', 'id': 0}],
                'connector': {'state': 'RUNNING', 'worker_id': '172.17.0.2:8083'}, 'type': 'source'}
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetConnectorStatus('dev', 'logs-broker'), recv)

    def test_GetConnectorTasks(self):
        recv = [{'config': {'file': '/var/log/broker.log',
                            'task.class': 'org.apache.kafka.connect.file.FileStreamSourceTask', 'topic': 'logs_broker'},
                 'id': {'connector': 'logs-broker', 'task': 0}}]
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetConnectorTasks('dev', 'logs-broker'), recv)

    def test_GetStatusTask(self):
        recv = {'state': 'RUNNING', 'worker_id': '172.17.0.2:8083', 'id': 0}
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetStatusTask('dev', 'logs-broker', '0'), recv)

    def test_RestartConnectorTask(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        try:
            conn.RestartConnector('dev', 'logs-broker')
        except Exception as e:
            raise AssertionError('Unexcepted raise exception:', e)

    def test_GetConnectorPlugins(self):
        recv = {'name': 'Couchbase', 'class': 'com.couchbase.connect.kafka.CouchbaseSinkConnector',
                'icon': 'couchbase.png', 'type': 'sink', 'description': 'Write Kafka data to Couchbase',
                'version': '3.2.2', 'author': 'Couchbase',
                'docs': '//developer.couchbase.com/documentation/server/current/connectors/kafka-3.1/quickstart.html',
                'uiEnabled': True}
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetConnectorPlugins('dev')[0], recv)

    def test_PauseConnector(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        try:
            conn.RestartConnector('dev', 'logs-broker')
        except Exception as e:
            raise AssertionError('Unexcepted raise exception:', e)

    def test_ResumeConnector(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        try:
            conn.ResumeConnector('dev', 'logs-broker')
        except Exception as e:
            raise AssertionError('Unexcepted raise exception:', e)

    def test_RestartConnector(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        try:
            conn.RestartConnector('dev', 'logs-broker')
        except Exception as e:
            raise AssertionError('Unexcepted raise exception:',e)


    # def test_CreateConnector(self):
    #     self.fail()
    #
    # def test_SetConnectorConfig(self):
    #     self.fail()
    #
    # def test_DeleteConnector(self):
    #     self.fail()
    #
    # def test_SubscribeHandler(self):
    #     self.fail()
    #
    # def test_Publish(self):
    #     self.fail()
    #
    # def test_Commit(self):
    #     self.fail()
    #
    # def test_Unscribe(self):
    #     self.fail()
    #
    # def test_Subscribe(self):
    #     self.fail()
    #
    def test_GetACLs(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetACLs(), [])

    def test_SetACL(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        try:
            conn.SetAcl("Topic", "transactions", "GROUPA:UserA", "Allow", "*", "Read")
        except Exception as e:
            raise AssertionError('Unexcepted raise exception:', e)

    def test_GetQuotas(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        self.assertEqual(conn.GetQuotas(), [])

    def test_SetQuotasAllUsers(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        config = {"producer_byte_rate": "100000",
                  "consumer_byte_rate": "200000",
                  "request_percentage": "75"
                  }
        try:
            conn.SetQuotasAllUsers(config)
        except Exception as e:
            raise AssertionError('Unexpected raise exception:', e)

    def test_SetQuotaUserAllClients(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        config = {"producer_byte_rate": "100000",
                  "consumer_byte_rate": "200000",
                  "request_percentage": "75"
                  }
        try:
            conn.SetQuotasAllUsers('admin', config)
        except Exception as e:
            raise AssertionError('Unexpected raise exception:', e)

    def test_SetQuotaUserClient(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        config = {"producer_byte_rate": "100000",
                  "consumer_byte_rate": "200000",
                  "request_percentage": "75"
                  }
        try:
            conn.SetQuotaUserClient('admin', 'admin', config)
        except Exception as e:
            raise AssertionError('Unexpected raise exception:', e)

    def test_SetQuotaUser(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        config = {"producer_byte_rate": "100000",
                  "consumer_byte_rate": "200000",
                  "request_percentage": "75"
                  }
        try:
            conn.SetQuotaUser("admin", config)
        except Exception as e:
            raise AssertionError('Unexpected raise exception:', e)

    def test_SetQuotaAllClient(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        config = {"producer_byte_rate": "100000",
                  "consumer_byte_rate": "200000",
                  "request_percentage": "75"
                  }
        try:
            conn.SetQuotaAllClient(config)
        except Exception as e:
            raise AssertionError('Unexpected raise exception:', e)

    def test_SetQuotaClient(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        config = {"producer_byte_rate": "100000",
                  "consumer_byte_rate": "200000",
                  "request_percentage": "75"
                  }
        try:
            conn.SetQuotaClient("admin", config)
        except Exception as e:
            raise AssertionError('Unexpected raise exception:', e)


    # def test_DeleteQutaAllUsers(self):
    #     self.fail()
    #
    # def test_DeleteQuotaUserAllClients(self):
    #     self.fail()
    #
    # def test_DeleteQuotaUserClient(self):
    #     self.fail()
    #
    # def test_DeleteQuotaUser(self):
    #     self.fail()
    #
    # def test_DeleteQuotaAllClients(self):
    #     self.fail()
    #
    # def test_DeleteQuotaClient(self):
    #     self.fail()
