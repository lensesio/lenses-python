class TestPolicy:

    def test_view_policy(self, lenses_conn):
        result = lenses_conn.ViewPolicy()
        assert type(result) is list

    def test_set_policy(self, lenses_conn):
        result = lenses_conn.SetPolicy("test_policy","All","HIGH","test_category",["test_field"])
        policies = lenses_conn.ViewPolicy()
        for e in policies:
            if e['name'] == 'test_policy':
                policy_id = e['id']
                break
        else:
            policy_id = None

        assert result == policy_id

    def test_del_policy(self, lenses_conn):
        assert lenses_conn.DelPolicy("test_policy") == 'OK'
