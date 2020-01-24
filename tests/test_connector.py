import json
import time

class TestConnector:

    def test_create_connector(self, lenses_conn):
        config = {
            "name": "test_connector",
            "config": {
                "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
                "tasks.max": "1",
                "topic": "test_connector_topic"
            }
        }
        result = lenses_conn.CreateConnector('dev', config)
        assert result['name'] == 'test_connector' and result['type'] == 'source'
        time.sleep(5)

    def test_list_all_connectors(self, lenses_conn):
        assert 'test_connector' in lenses_conn.GetConnectors('dev')

    def test_get_info_connector(self, lenses_conn):
        result = lenses_conn.GetConnectorInfo('dev', 'test_connector')
        assert result['name'] == 'test_connector' and result['config']['tasks.max'] == '1'

    def test_get_connector_config(self, lenses_conn):
        result = lenses_conn.GetConnectorConfig('dev', 'test_connector')
        assert result['name'] == 'test_connector' and result['tasks.max'] == '1' and result['topic'] == 'test_connector_topic'

    def test_get_connector_status(self, lenses_conn):
        assert 'test_connector' in lenses_conn.GetConnectorStatus('dev', 'test_connector')['name']

    def test_get_connector_tasks(self, lenses_conn):
        result = lenses_conn.GetConnectorTasks('dev', 'test_connector')
        assert result[0]['id']['connector'] == 'test_connector'

    def test_get_status_task(self, lenses_conn):
        assert lenses_conn.GetStatusTask('dev', 'test_connector', '0')['id'] == 0

    def test_restart_connector_task(self, lenses_conn):
        assert lenses_conn.RestartConnectorTask('dev', 'test_connector', '0') == ''

    def test_get_connector_plugins(self, lenses_conn):
        result = lenses_conn.GetConnectorPlugins('dev')
        assert 'class' in result[0]

    def test_pause_connector(self, lenses_conn):
        assert lenses_conn.PauseConnector('dev', 'test_connector') == ''

    def test_resume_connector(self, lenses_conn):
        assert lenses_conn.ResumeConnector('dev', 'test_connector') == ''

    def test_restart_connector(self, lenses_conn):
        assert lenses_conn.RestartConnector('dev', 'test_connector') == ''

    def test_set_connector_config(self, lenses_conn):
        config = {
            "name": "test_connector",
            "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "tasks.max": "5",
            "topic": "test_connector_topic"
        }
        result = lenses_conn.SetConnectorConfig('dev', 'test_connector', config)
        assert result['config']['tasks.max'] == '5'

    def test_delete_connector(self, lenses_conn):
        assert lenses_conn.DeleteConnector('dev', 'test_connector') == ''
