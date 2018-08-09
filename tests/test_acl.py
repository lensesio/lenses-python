from lenses_python.lenses import lenses


class TestAcl:

    def setup(self):
        self.conn = lenses("http://localhost:3030", "admin", "admin")

    def teardown(self):
        self.conn = None

    def test_get_acls(self):
        assert self.conn.GetACLs()

    def test_set_acl(self):
        self.conn.SetACL("Topic", "transactions", "GROUPA:UserA", "Allow", "*", "Read")
