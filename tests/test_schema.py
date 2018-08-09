from __builtin__ import isinstance, dict

from lenses_python.lenses import lenses

SCHEMA_CONFIG = {
    'schema':
        '{"type":"record","name":"reddit_post_key",'
        '"namespace":"com.landoop.social.reddit.post.key",'
        '"fields":[{"name":"testit_id","type":"string"}]}'
}
COMPATIBILITY_CONFIG = {'compatibility': 'BACKWARD'}


class TestSchema:

    def setup(self):
        self.conn = lenses("http://localhost:3030", "admin", "admin")

    def teardown(self):
        self.conn = None

    def test_get_all_subjects(self):
        assert self.conn.GetAllSubjects()

    def test_list_versions_subj(self):
        subj = self.conn.GetAllSubjects()[0]
        assert self.conn.ListVersionsSubj(subj)

    def test_get_schema_by_id(self):
        schema_id = str(self.conn.RegisterNewSchema("test_schema", SCHEMA_CONFIG)['id'])
        assert 'schema' in self.conn.GetSchemaById(schema_id).keys()

    def test_get_schema_by_ver(self):
        subj = self.conn.GetAllSubjects()[0]
        assert isinstance(self.conn.GetSchemaByVer(subj, '1'), dict)

    def test_register_new_schema(self):
        assert 'id' in self.conn.RegisterNewSchema("test", SCHEMA_CONFIG).keys()

    def test_get_global_compatibility(self):
        config = {'compatibilityLevel': 'BACKWARD'}
        result = self.conn.GetGlobalCompatibility()
        assert config == result

    def test_get_compatibility(self):
        subj = self.conn.GetAllSubjects()[0]
        self.conn.ChangeCompatibility(subj, COMPATIBILITY_CONFIG)
        assert 'compatibilityLevel' in self.conn.GetCompatibility(subj).keys()

    def test_delete_subj(self):
        subj = self.conn.GetAllSubjects()[0]
        self.conn.DeleteSubj(subj)

    def test_delete_schema_by_version(self):
        self.conn.DeleteSchemaByVersion("cc_payments-value", '1')

    def test_change_compatibility(self):
        subj = "cc_payments-value"
        self.conn.ChangeCompatibility(subj, COMPATIBILITY_CONFIG)

    def test_update_global_compatibility(self):
        self.conn.UpdateGlobalCompatibility(COMPATIBILITY_CONFIG)
