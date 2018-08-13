class TestLenses:

    def test_GetCredentials(self, lenses_conn):
        result = lenses_conn.GetCredentials()['user']
        assert 'admin' == result
