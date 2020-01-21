import time

class TestProcessor:

    def test_create_topic_for_lsql(self, lenses_conn):
        config = {
            "cleanup.policy": "compact",
            "compression.type": "snappy"
        }
        result = lenses_conn.CreateTopic("test_processor_source", 1, 1, config)
        tmp = ['Topic [test_processor_source] created']
        tmp.append('Requested to create topic test_processor_source - it already exists')
        assert result in tmp

    def test_create_processor(self, lenses_conn):
        time.sleep(10)
        query = (
            "SET autocreate=true; insert into test_processor_target SELECT * FROM test_processor_source"
        )
        result = lenses_conn.CreateProcessor("test_processor", query, 1, 'dev', 'ns', '1').split('_')[0]
        assert "lsql" in result

    def test_pause_processor(self, lenses_conn):
        processor_id = lenses_conn.GetProcessorID('test_processor')
        assert lenses_conn.PauseProcessor(processor_id[0]) == 'OK'

    def test_resume_processor(self, lenses_conn):
        processor_id = lenses_conn.GetProcessorID('test_processor')
        assert lenses_conn.ResumeProcessor(processor_id[0]) == 'OK'

    def test_update_processor(self, lenses_conn):
        processor_id = lenses_conn.GetProcessorID('test_processor')
        assert lenses_conn.UpdateProcessorRunners(processor_id[0], '4') == 'OK'

    def test_delete_processor(self, lenses_conn):
        processor_id = lenses_conn.GetProcessorID('test_processor')
        assert lenses_conn.DeleteProcessor(processor_id[0]) == 'OK'