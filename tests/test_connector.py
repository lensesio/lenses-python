class TestConnector:

    def test_list_all_connectors(self, lenses_conn):
        recv = ['logs-broker', 'nullsink']
        assert recv == lenses_conn.ListAllConnectors('dev')

    def test_get_info_connector(self, lenses_conn):
        recv = {
            'name': 'logs-broker', 'tasks': [{'connector': 'logs-broker', 'task': 0}],
            'config': {
                'file': '/var/log/broker.log', 'name': 'logs-broker', 'topic': 'logs_broker',
                'connector.class': 'org.apache.kafka.connect.file.FileStreamSourceConnector',
                'tasks.max': '1'
            }, 'type': 'source'
        }
        result = lenses_conn.GetInfoConnector('dev', 'logs-broker')
        assert result == recv

    def test_get_connector_config(self, lenses_conn):
        recv = {
            'file': '/var/log/broker.log',
            'name': 'logs-broker',
            'topic': 'logs_broker',
            'connector.class': 'org.apache.kafka.connect.file.FileStreamSourceConnector',
            'tasks.max': '1'
        }
        result = lenses_conn.GetConnectorConfig('dev', 'logs-broker')

        assert result == recv

    def test_get_connector_status(self, lenses_conn):
        assert 'logs-broker' in lenses_conn.GetConnectorStatus('dev', 'logs-broker')['name']

    def test_get_connector_tasks(self, lenses_conn):
        recv = [{'config': {
            'batch.size': '2000',
            'file': '/var/log/broker.log',
            'task.class': 'org.apache.kafka.connect.file.FileStreamSourceTask',
            'topic': 'logs_broker'
        },
            'id': {'connector': 'logs-broker', 'task': 0}}]

        result = lenses_conn.GetConnectorTasks('dev', 'logs-broker')
        assert result == recv

    def test_get_status_task(self, lenses_conn):
        assert 'RUNNING' in lenses_conn.GetStatusTask('dev', 'logs-broker', '0')['state']

    def test_restart_connector_task(self, lenses_conn):
        lenses_conn.RestartConnector('dev', 'logs-broker')

    def test_get_connector_plugins(self, lenses_conn):
        recv = {
            'name': 'Couchbase', 'class': 'com.couchbase.connect.kafka.CouchbaseSinkConnector',
            'icon': 'couchbase.png', 'type': 'sink', 'description': 'Write Kafka data to Couchbase',
            'version': '3.2.2', 'author': 'Couchbase',
            'docs': '//developer.couchbase.com/documentation/server/current/connectors/kafka-3.1/quickstart.html',
            'uiEnabled': True
        }
        result = lenses_conn.GetConnectorPlugins('dev')[0]

        assert recv == result

    def test_pause_connector(self, lenses_conn):
        lenses_conn.RestartConnector('dev', 'logs-broker')

    def test_resume_connector(self, lenses_conn):
        lenses_conn.ResumeConnector('dev', 'logs-broker')

    def test_restart_connector(self, lenses_conn):
        lenses_conn.RestartConnector('dev', 'logs-broker')

    def test_create_connector(self, lenses_conn):
        config = {'config': {
            'connect.coap.kcql': '1',
            'topics': 'reddit_posts',
            'connector.class':
                'com.datamountaineer.streamreactor.connect.coap.sink.CoapSinkConnector'
        },
            'name': 'test_connector'
        }
        recv = {'config': {
            'connect.coap.kcql': '1',
            'connector.class': 'com.datamountaineer.streamreactor.connect.coap.sink.CoapSinkConnector',
            'name': 'test_connector',
            'topics': 'reddit_posts',
        },
            'tasks': [], 'type': None, 'name': 'test_connector'
        }
        assert recv == lenses_conn.CreateConnector('dev', config)

    def test_set_connector_config(self, lenses_conn):
        config = {'connector.class': 'org.apache.kafka.connect.file.FileStreamSinkConnector',
                  'task.max': 5,
                  'topics': 'nyc_yellow_taxi_trip_data,reddit_posts,sea_vessel_position_reports,telecom_italia_data',
                  'file': '/dev/null',
                  'tasks.max': '4',
                  'name': 'nullsink'}
        lenses_conn.SetConnectorConfig('dev', 'nullsink', config)

    def test_delete_connector(self, lenses_conn):
        lenses_conn.DeleteConnector('dev', 'test_connector')
