from lenses_python.lenses import lenses


class TestProcessor:

    def setup(self):
        self.conn = lenses("http://localhost:3030", "admin", "admin")

    def teardown(self):
        self.conn = None

    def test_create_processor(self):
        query = (
            " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and "
            "_ktype=AVRO and _vtype=AVRO "
        )

        result = self.conn.CreateProcessor("test_processor", query, 1, 'dev', 'ns', '1').split('_')[0]
        assert "lsql" in result

    def test_delete_processor(self):
        query = (
            " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and "
            "_ktype=AVRO and _vtype=AVRO "
        )

        processor_id = self.conn.CreateProcessor("test_processor_2", query, 1, 'dev', 'ns', '1')
        self.conn.DeleteProcessor(processor_id)

    def test_resume_processor(self):
        query = (
            " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and "
            "_ktype=AVRO and _vtype=AVRO "
        )
        processor_id = self.conn.CreateProcessor("test_processor_3", query, 1, 'dev', 'ns', '1')
        self.conn.ResumeProcessor(processor_id)

    def test_pause_processor(self):
        query = (
            " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and "
            "_ktype=AVRO and _vtype=AVRO "
        )
        processor_id = self.conn.CreateProcessor("test_processor_4", query, 1, 'dev', 'ns', '1')
        self.conn.PauseProcessor(processor_id)

    def test_update_processor(self):
        query = (
            " SET autocreate=true; insert into body SELECT  body FROM  `reddit_posts` WHERE score> 10 and "
            "_ktype=AVRO and _vtype=AVRO "
        )
        processor_id = self.conn.CreateProcessor("test_processor_5", query, 1, 'dev', 'ns', '1')
        self.conn.UpdateProcessorRunners(processor_id, '4')
