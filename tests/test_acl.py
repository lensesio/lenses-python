class TestAcl:

    def test_get_acls_init(self, lenses_conn):
        assert type(lenses_conn.GetAcl()) is list

    def test_set_acl(self, lenses_conn):
        assert lenses_conn.SetAcl("TOPIC", "TRANSACTIONS", "GROUPA:UserA", "ALLOW", "*", "READ") == ''

    def test_get_acls(self, lenses_conn):
        assert lenses_conn.GetAcl()[0]['resourceType'] == 'TOPIC' and lenses_conn.GetAcl()[0]['operation'] == 'READ'

    def test_del_acl(self, lenses_conn):
        lenses_conn.DelAcl("TOPIC", "TRANSACTIONS", "GROUPA:UserA", "ALLOW", "*", "READ") == 'OK'

    def test_get_acls_init(self, lenses_conn):
        assert type(lenses_conn.GetAcl()) is list
