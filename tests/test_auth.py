from lenses_python.lenses import lenses


class TestLenses:
    def setup(self):
        self.conn = lenses("http://localhost:3030", "admin", "admin")

    def teardown(self):
        self.conn = None

    def test_GetCredentials(self):
        result = self.conn.GetCredentials()['user']
        assert 'admin' == result
