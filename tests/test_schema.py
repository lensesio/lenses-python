from builtins import isinstance, dict

SCHEMA_CONFIG = {
    'schema':
        '{"type":"record","name":"reddit_post_key",'
        '"namespace":"com.landoop.social.reddit.post.key",'
        '"fields":[{"name":"testit_id","type":"string"}]}'
}
COMPATIBILITY_CONFIG = {'compatibility': 'BACKWARD'}


class TestSchema:

    def test_get_all_subjects(self, lenses_conn):
        assert lenses_conn.GetAllSubjects()

    def test_list_versions_subj(self, lenses_conn):
        subj = lenses_conn.GetAllSubjects()[0]
        assert lenses_conn.ListVersionsSubj(subj)

    def test_get_schema_by_id(self, lenses_conn):
        schema_id = str(lenses_conn.RegisterNewSchema("test_schema", SCHEMA_CONFIG)['id'])
        assert 'schema' in lenses_conn.GetSchemaById(schema_id).keys()

    def test_get_schema_by_ver(self, lenses_conn):
        subj = lenses_conn.GetAllSubjects()[0]
        assert isinstance(lenses_conn.GetSchemaByVer(subj, '1'), dict)

    def test_register_new_schema(self, lenses_conn):
        assert 'id' in lenses_conn.RegisterNewSchema("test", SCHEMA_CONFIG).keys()

    def test_get_global_compatibility(self, lenses_conn):
        config = {'compatibilityLevel': 'BACKWARD'}
        result = lenses_conn.GetGlobalCompatibility()
        assert config == result

    def test_get_compatibility(self, lenses_conn):
        subj = lenses_conn.GetAllSubjects()[0]
        lenses_conn.ChangeCompatibility(subj, COMPATIBILITY_CONFIG)
        assert 'compatibilityLevel' in lenses_conn.GetCompatibility(subj).keys()

    def test_delete_subj(self, lenses_conn):
        subj = lenses_conn.GetAllSubjects()[0]
        lenses_conn.DeleteSubj(subj)

    def test_delete_schema_by_version(self, lenses_conn):
        lenses_conn.DeleteSchemaByVersion("cc_payments-value", '1')

    def test_change_compatibility(self, lenses_conn):
        subj = "cc_payments-value"
        lenses_conn.ChangeCompatibility(subj, COMPATIBILITY_CONFIG)

    def test_update_global_compatibility(self, lenses_conn):
        lenses_conn.UpdateGlobalCompatibility(COMPATIBILITY_CONFIG)
