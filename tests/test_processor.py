class TestProcessor:

    def test_create_processor(self, lenses_conn):
        query = (
            " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and "
            "_ktype=AVRO and _vtype=AVRO "
        )

        result = lenses_conn.CreateProcessor("test_processor", query, 1, 'dev', 'ns', '1').split('_')[0]
        assert "lsql" in result

    def test_delete_processor(self, lenses_conn):
        query = (
            " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and "
            "_ktype=AVRO and _vtype=AVRO "
        )

        processor_id = lenses_conn.CreateProcessor("test_processor_2", query, 1, 'dev', 'ns', '1')
        lenses_conn.DeleteProcessor(processor_id)

    def test_resume_processor(self, lenses_conn):
        query = (
            " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and "
            "_ktype=AVRO and _vtype=AVRO "
        )
        processor_id = lenses_conn.CreateProcessor("test_processor_3", query, 1, 'dev', 'ns', '1')
        lenses_conn.ResumeProcessor(processor_id)

    def test_pause_processor(self, lenses_conn):
        query = (
            " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and "
            "_ktype=AVRO and _vtype=AVRO "
        )
        processor_id = lenses_conn.CreateProcessor("test_processor_4", query, 1, 'dev', 'ns', '1')
        lenses_conn.PauseProcessor(processor_id)

    def test_update_processor(self, lenses_conn):
        query = (
            " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and "
            "_ktype=AVRO and _vtype=AVRO "
        )
        processor_id = lenses_conn.CreateProcessor("test_processor_5", query, 1, 'dev', 'ns', '1')
        lenses_conn.UpdateProcessorRunners(processor_id, '4')
