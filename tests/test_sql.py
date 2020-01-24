class TestSql:

    def test_sql_create_table(self, lenses_conn):
        query = (
            "CREATE TABLE greetings(_key string, _value string) FORMAT (string, string)"
        )
        result = lenses_conn.ExecSQL(query)
        assert result['data'][0]['value']['flag']

    def test_sql_insert_data(self, lenses_conn):
        query = (
            "INSERT INTO greetings(_key, _value) VALUES('Hello', 'World')"
        )
        result = lenses_conn.ExecSQL(query)
        assert result['data'][0]['value']['flag'] == True

    def test_sql_query_data(self, lenses_conn):
        query = (
            "SELECT * FROM greetings limit 1"
        )
        result = lenses_conn.ExecSQL(query)
        assert 'World' in result['data'][0]['value']

    def test_sql_drop_table(self, lenses_conn):
        query = (
            "DROP TABLE greetings"
        )
        result = lenses_conn.ExecSQL(query)
        assert result['data'][0]['value']
