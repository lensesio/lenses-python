class TestPolicy:

    def test_view_policy(self, lenses_conn):
        result = lenses_conn.view_policy()
        assert result == []
