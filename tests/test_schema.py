from builtins import isinstance, dict

SCHEMA_CONFIG = {
    'schema':
        '{"type":"record","name":"reddit_post_key",'
        '"namespace":"com.landoop.social.reddit.post.key",'
        '"fields":[{"name":"testit_id","type":"string"}]}'
}
COMPATIBILITY_CONFIG = {'compatibility': 'BACKWARD'}
COMPATIBILITY_CONFIG_UPDATE = {'compatibility': 'FULL'}


class TestSchema:

    def test_get_all_subjects(self, lenses_conn):
        assert lenses_conn.GetAllSubjects()

    def test_register_new_schema(self, lenses_conn):
        assert 'id' in lenses_conn.RegisterNewSchema('test_schema', SCHEMA_CONFIG).keys()

    def test_list_versions_subj(self, lenses_conn):
        assert lenses_conn.ListVersionsSubj('test_schema')

    def test_get_schema_by_id(self, lenses_conn):
        schema_id = lenses_conn.RegisterNewSchema('test_schema', SCHEMA_CONFIG)['id']
        assert 'schema' in lenses_conn.GetSchemaById(schema_id).keys()

    def test_get_global_compatibility(self, lenses_conn):
        result = lenses_conn.GetGlobalCompatibility()
        assert 'compatibilityLevel' in result.keys()

    def test_update_global_compatibility(self, lenses_conn):
        assert 'compatibility' in lenses_conn.UpdateGlobalCompatibility(COMPATIBILITY_CONFIG_UPDATE).keys()

    def test_change_compatibility(self, lenses_conn):
        lenses_conn.ChangeCompatibility('test_schema', COMPATIBILITY_CONFIG_UPDATE)

    def test_get_compatibility(self, lenses_conn):
        assert 'compatibilityLevel' in lenses_conn.GetCompatibility('test_schema').keys()

    def test_update_schema(self, lenses_conn):
        SCHEMA_CONFIG_UPDATE = {
            'schema':
                '{"type":"record","name":"reddit_post_key",'
                '"namespace":"com.landoop.social.reddit.post.key",'
                '"fields":[{"name":"testit_id","type":"string","doc":"desc."}]}'
        }
        subj = lenses_conn.UpdateSchema('test_schema', SCHEMA_CONFIG_UPDATE)
        assert 'id' in subj.keys()

    def test_get_schema_by_ver(self, lenses_conn):
        subj_ver = lenses_conn.ListVersionsSubj('test_schema')[-1]
        assert 'test_schema' == lenses_conn.GetSchemaByVer('test_schema', subj_ver)['subject']

    def test_delete_schema_by_version(self, lenses_conn):
        subj_ver = lenses_conn.ListVersionsSubj('test_schema')[-1]
        assert lenses_conn.DeleteSchemaByVersion("test_schema", subj_ver) == str(subj_ver)

    def test_delete_subj(self, lenses_conn):
        subj_ver = lenses_conn.ListVersionsSubj('test_schema')
        assert lenses_conn.DeleteSubj("test_schema") == subj_ver
