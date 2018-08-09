import multiprocessing
import time

from pandas._libs import json

from lenses_python.lenses import lenses


class TestTopic:

    def test_get_all_topics(self, lenses_conn):
        assert '_kafka_lenses_lsql_storage' in lenses_conn.GetAllTopics()[0]['topicName']

    def test_topic_info(self, lenses_conn):
        topic_name = '_kafka_lenses_lsql_storage'
        assert topic_name in lenses_conn.TopicInfo(topic_name)['topicName']

    def test_topic_names(self, lenses_conn):
        assert lenses_conn.TopicsNames()

    def test_update_topic_config(self, lenses_conn):
        config = {"configs": [{"key": "cleanup.policy", "value": "compact"}]}
        lenses_conn.UpdateTopicConfig('_kafka_lenses_lsql_storage', config)

    def test_create_topic(self, lenses_conn):
        config = {
            "cleanup.policy": "compact",
            "compression.type": "snappy"
        }
        lenses_conn.CreateTopic("test_topic", 1, 1, config)

    def test_delete_topic(self, lenses_conn):
        lenses_conn.DeleteTopic("test_topic")

    def test_delete_topic_records(self, lenses_conn):
        topic = lenses_conn.TopicsNames()[0]
        msg = "Records from topic '%s' and partition '0' up to offset '10'" % topic
        response = lenses_conn.DeleteTopicRecords(topic, "0", "10")
        assert msg in response.split(',')[0]

    def test_websocket_handler(self, lenses_conn):
        config = {
            "cleanup.policy": "compact",
            "compression.type": "snappy"
        }
        lenses_conn.CreateTopic("test_topic_ws", 1, 1, config)
        p = multiprocessing.Process(target=self.subscribe_to_topic)
        p.start()
        self.publish_to_topic()
        time.sleep(20)
        p.terminate()
        read_file = open('test_file')
        assert 1 == json.load(read_file)[0]["value"]
        read_file.close()

    def publish_to_topic(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        url = 'ws://localhost:3030'
        conn.Publish(url, "admin", "test_topic_ws", "None", "{'value':1}")

    def subscribe_to_topic(self):
        conn = lenses("http://localhost:3030", "admin", "admin")
        query = (
            "SELECT * FROM `test_topic_ws` WHERE _vtype='STRING' AND _ktype='STRING' AND _sample=2 AND _sampleWindow=200"
        )
        url = 'ws://localhost:3030'
        conn.SubscribeHandler(url, "admin", query, write=True, filename='test_file')
