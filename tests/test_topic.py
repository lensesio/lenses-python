import multiprocessing
import time

from pandas._libs import json

from lenses_python.lenses import lenses


class TestTopic:

    def setup(self):
        self.conn = lenses("http://localhost:3030", "admin", "admin")

    def teardown(self):
        self.conn = None

    def test_get_all_topics(self):
        assert '_kafka_lenses_lsql_storage' in self.conn.GetAllTopics()[0]['topicName']

    def test_topic_info(self):
        topic_name = '_kafka_lenses_lsql_storage'
        assert topic_name in self.conn.TopicInfo(topic_name)['topicName']

    def test_topic_names(self):
        assert self.conn.TopicsNames()

    def test_update_topic_config(self):
        config = {"configs": [{"key": "cleanup.policy", "value": "compact"}]}
        self.conn.UpdateTopicConfig('_kafka_lenses_lsql_storage', config)

    def test_create_topic(self):
        config = {
            "cleanup.policy": "compact",
            "compression.type": "snappy"
        }
        self.conn.CreateTopic("test_topic", 1, 1, config)

    def test_delete_topic(self):
        self.conn.DeleteTopic("test_topic")

    def test_delete_topic_records(self):
        topic = self.conn.TopicsNames()[0]
        msg = "Records from topic '%s' and partition '0' up to offset '10'" % topic
        response = self.conn.DeleteTopicRecords(topic, "0", "10")
        assert msg in response.split(',')[0]

    def test_websocket_handler(self):
        self._publish_to_topic()
        p = multiprocessing.Process(target=self._subscribe_to_topic)
        p.start()
        time.sleep(5)
        p.terminate()
        read_file = open("test_file")
        assert 1 in json.load(read_file)[0]["value"]
        read_file.close()

    def _publish_to_topic(self):
        url = 'ws://localhost:3030'
        self.conn.Publish(url, "admin", "test_topic", "None", "{'value':1}")

    def _subscribe_to_topic(self):
        query = (
            "SELECT * FROM `test_topic` WHERE _vtype='STRING' AND _ktype='STRING' AND _sample=2 AND _sampleWindow=200"
        )
        url = 'ws://localhost:3030'
        self.conn.SubscribeHandler(url, "admin", query, write=True, filename="test_file")
