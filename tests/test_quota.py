QUOTA_CONFIG = {
    "producer_byte_rate": "100000",
    "consumer_byte_rate": "200000",
    "request_percentage": "75"
}


class TestQuota:

    def test_get_quotas(self, lenses_conn):
        assert lenses_conn.GetQuotas() == []

    def test_set_quotas_all_users(self, lenses_conn):
        assert lenses_conn.SetQuotasAllUsers(QUOTA_CONFIG) == 'OK'

    def test_set_quota_user_all_clients(self, lenses_conn):
        assert lenses_conn.SetQuotasAllUsers(QUOTA_CONFIG) == 'OK'

    def test_set_quota_user_client(self, lenses_conn):
        assert lenses_conn.SetQuotaUserClient('admin', 'admin', QUOTA_CONFIG) == 'OK'

    def test_set_quota_user(self, lenses_conn):
        assert lenses_conn.SetQuotaUser("admin", QUOTA_CONFIG) == 'OK'

    def test_set_quota_all_client(self, lenses_conn):
        assert lenses_conn.SetQuotaAllClient(QUOTA_CONFIG) == 'OK'

    def test_set_quota_client(self, lenses_conn):
        assert lenses_conn.SetQuotaClient("admin", QUOTA_CONFIG) == 'OK'

    def test_delete_quta_all_users(self, lenses_conn):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        assert lenses_conn.DeleteQutaAllUsers(config) == 'OK'

    def test_delete_quota_user_all_clients(self, lenses_conn):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        assert lenses_conn.DeleteQuotaUserAllClients("admin", config) == 'OK'

    def test_delete_quota_user_client(self, lenses_conn):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        assert lenses_conn.DeleteQuotaUserClient("admin", "admin", config) == 'OK'

    def test_delete_quota_user(self, lenses_conn):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        assert lenses_conn.DeleteQuotaUser("admin", config) == 'OK'

    def test_delete_quota_all_clients(self, lenses_conn):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        assert lenses_conn.DeleteQuotaAllClients(config) == 'OK'

    def test_delete_quota_client(self, lenses_conn):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        assert lenses_conn.DeleteQuotaClient('admin', config) == 'OK'
