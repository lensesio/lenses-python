from lenses_python.lenses import lenses

QUOTA_CONFIG = {
    "producer_byte_rate": "100000",
    "consumer_byte_rate": "200000",
    "request_percentage": "75"
}


class TestQuota:

    def setup(self):
        self.conn = lenses("http://localhost:3030", "admin", "admin")

    def teardown(self):
        self.conn = None

    def test_get_quotas(self):
        assert not self.conn.GetQuotas()

    def test_set_quotas_all_users(self):
        self.conn.SetQuotasAllUsers(QUOTA_CONFIG)

    def test_set_quota_user_all_clients(self):
        self.conn.SetQuotasAllUsers(QUOTA_CONFIG)

    def test_set_quota_user_client(self):
        self.conn.SetQuotaUserClient('admin', 'admin', QUOTA_CONFIG)

    def test_set_quota_user(self):
        self.conn.SetQuotaUser("admin", QUOTA_CONFIG)

    def test_set_quota_all_client(self):
        self.conn.SetQuotaAllClient(QUOTA_CONFIG)

    def test_set_quota_client(self):
        self.conn.SetQuotaClient("admin", QUOTA_CONFIG)

    def test_delete_quta_all_users(self):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        self.conn.DeleteQutaAllUsers(config)

    def test_delete_quota_user_all_clients(self):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        self.conn.DeleteQuotaUserAllClients("admin", config)

    def test_delete_quota_user_client(self):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        self.conn.DeleteQuotaUserClient("admin", "admin", config)

    def test_delete_quota_user(self):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        self.conn.DeleteQuotaUser("admin", config)

    def test_delete_quota_all_clients(self):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        self.conn.DeleteQuotaAllClients(config)

    def test_delete_quota_client(self):
        config = ['consumer_byte_rate', 'producer_byte_rate', 'request_percentage']
        self.conn.DeleteQuotaClient('admin', config)
