class TestAcl:

    def test_get_acls(self, lenses_conn):
        assert not lenses_conn.GetACLs()

    def test_set_acl(self, lenses_conn):
        lenses_conn.SetACL("Topic", "transactions", "GROUPA:UserA", "Allow", "*", "Read")
