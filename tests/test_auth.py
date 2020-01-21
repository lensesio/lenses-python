class TestLenses:

    def test_GetCredentials(self, lenses_conn):
        result = lenses_conn.UserInfo()['token']
        assert type(result) is str and len(result) >= 36
