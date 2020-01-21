import multiprocessing
import time

import json

from lensesio.lenses import main


class TestTopic:

    def test_create_topic(self, lenses_conn):
        config = {
            "cleanup.policy": "compact",
            "compression.type": "snappy"
        }
        lenses_conn.CreateTopic("test_topic", 1, 1, config)

    def test_get_all_topics(self, lenses_conn):
        time.sleep(10)
        assert 'topicName' in lenses_conn.GetAllTopics()[0].keys()

    def test_topic_info(self, lenses_conn):
        topic_name = 'test_topic'
        assert topic_name == lenses_conn.TopicInfo(topic_name)['topicName']

    def test_topic_names(self, lenses_conn):
        topics_list = lenses_conn.LstOfTopicsNames()
        assert type(topics_list) is list and 'test_topic' in topics_list

    def test_update_topic_config(self, lenses_conn):
        config = {"configs": [{"key": "cleanup.policy", "value": "compact"}]}
        result = lenses_conn.UpdateTopicConfig('test_topic', config)
        assert result == 'Topic [test_topic] updated config with [SetTopicConfiguration(List(TopicConfigKeyValue(cleanup.policy,compact)))]'

    def publish_to_topic(self):
        conn.Publish("test_topic", "test_key", "{'value':1}")

    def test_delete_topic_records(self, lenses_conn):
        msg = "Records from topic '%s' and partition '0' up to offset '10'" % 'test_topic'
        response = lenses_conn.DeleteTopicRecords('test_topic', "0", "10")
        assert msg in response.split(',')

    def test_delete_topic(self, lenses_conn):
        lenses_conn.DeleteTopic("test_topic")