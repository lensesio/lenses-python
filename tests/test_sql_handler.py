class TestSqlHandler:

    def test_SqlHandler(self, lenses_conn):
        query = (
            "SELECT * FROM reddit_posts limit 1"
        )

        result = lenses_conn.SqlHandler(query)
        assert result['records'][0]['value']
