class TestAcl:

    def test_get_acls_init(self, lenses_conn):
        assert type(lenses_conn.GetAcl()) is list

    def test_set_acl(self, lenses_conn):
        lenses_conn.SetAcl("Topic", "transactions", "GROUPA:UserA", "Allow", "*", "Read") == 'OK'

    def test_get_acls(self, lenses_conn):
        assert lenses_conn.GetAcl()[0]['resourceType'] == 'TOPIC' and lenses_conn.GetAcl()[0]['operation'] == 'READ'

    def test_del_acl(self, lenses_conn):
        lenses_conn.DelAcl("Topic", "transactions", "GROUPA:UserA", "Allow", "*", "Read") == 'OK'

    def test_get_acls_init(self, lenses_conn):
        assert type(lenses_conn.GetAcl()) is list
