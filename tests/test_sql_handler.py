class TestSqlHandler:

    def test_sqlhandler_create_table(self, lenses_conn):
        query = (
            "CREATE TABLE greetings(_key string, _value string) FORMAT (string, string)"
        )

        result = lenses_conn.SqlHandler(query)
        assert 'Topic greetings has been created' in result['records'][0]['value']

    def test_sqlhandler_insert_data(self, lenses_conn):
        query = (
            "INSERT INTO greetings(_key, _value) VALUES('Hello', 'World')"
        )

        result = lenses_conn.SqlHandler(query)
        assert result['records'][0]['value'] == True

    def test_sqlhandler_query_data(self, lenses_conn):
        query = (
            "SELECT * FROM greetings limit 1"
        )

        result = lenses_conn.SqlHandler(query)
        assert 'World' in result['records'][0]['value']

    def test_sqlhandler_drop_table(self, lenses_conn):
        query_dt = (
            "DROP TABLE greetings"
        )

        result_dt = lenses_conn.SqlHandler(query_dt)

        assert type(result_dt['records']) is list

        query_st = (
            "SHOW TABLES"
        )

        result_st = lenses_conn.SqlHandler(query_st)

        status=True
        for rec in result_st['records']:
            if rec['value']['name'] == 'greetings':
                status=False
                break

        assert status == True
