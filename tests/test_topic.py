import multiprocessing
import time

import json

from lensesio.lenses import main


class TestTopic:

    def test_create_topic(self, lenses_conn):
        config = {
            "cleanup.policy": "delete",
            "compression.type": "snappy"
        }
        assert lenses_conn.CreateTopic("test_topic", 1, 1, config) == 'Topic [test_topic] created'

    def test_get_all_topics(self, lenses_conn):
        time.sleep(10)
        assert 'topicName' in lenses_conn.GetAllTopics()[0].keys()

    def test_topic_info(self, lenses_conn):
        topic_name = 'test_topic'
        assert topic_name == lenses_conn.TopicInfo(topic_name)['topicName']

    def test_topic_names(self, lenses_conn):
        topics_list = lenses_conn.LstOfTopicsNames()
        assert type(topics_list) is list and 'test_topic' in topics_list

    def test_sql_insert_to_topic(self, lenses_conn):
        query = (
            "INSERT INTO test_topic(_key, _value) VALUES('test_key', '1')"
        )
        result = lenses_conn.ExecSQL(query)
        assert result['data'][0]['value']['flag'] == True

    def test_delete_topic_records(self, lenses_conn):
        msg = "Records from topic '%s' and partition '0' up to offset '10'" % 'test_topic'
        response = lenses_conn.DeleteTopicRecords('test_topic', "0", "10")
        assert msg in response.split(',')

    def test_delete_topic(self, lenses_conn):
        assert lenses_conn.DeleteTopic("test_topic") == "Topic 'test_topic' has been marked for deletion"

    def test_create_topic_compact(self, lenses_conn):
        config = {
            "cleanup.policy": "compact",
            "compression.type": "snappy"
        }
        assert lenses_conn.CreateTopic("test_topic_compact", 1, 1, config) == 'Topic [test_topic_compact] created'

    def test_update_topic_config(self, lenses_conn):
        config = {"configs": [{"key": "cleanup.policy", "value": "delete"}]}
        result = lenses_conn.UpdateTopicConfig('test_topic_compact', config)
        assert result == 'Topic [test_topic_compact] updated config with [SetTopicConfiguration(List(TopicConfigKeyValue(cleanup.policy,delete)))]'

    def test_delete_topic_compact(self, lenses_conn):
        assert lenses_conn.DeleteTopic("test_topic_compact") == "Topic 'test_topic_compact' has been marked for deletion"