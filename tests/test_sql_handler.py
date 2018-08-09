from lenses_python.lenses import lenses


class TestSqlHandler:
    def setup(self):
        self.conn = lenses("http://localhost:3030", "admin", "admin")

    def teardown(self):
        self.conn = None

    def test_SqlHandler(self):
        recv = {
            'messages': [
                {'value': '{"VendorID":2,"tpep_pickup_datetime":"2016-01-01 00:00:00",'
                          '"tpep_dropoff_datetime":"2016-01-01 00:00:00","passenger_count":2,'
                          '"trip_distance":1.1,"pickup_longitude":-73.99037170410156,'
                          '"pickup_latitude":40.73469543457031,"RateCodeID":1,"store_and_fwd_flag":"N",'
                          '"dropoff_longitude":-73.98184204101562,"dropoff_latitude":40.73240661621094,'
                          '"payment_type":2,"fare_amount":7.5,"extra":0.5,"mta_tax":0.5,'
                          '"improvement_surcharge":0.3,"tip_amount":0.0,"tolls_amount":0.0,'
                          '"total_amount":8.8}', 'key': None, 'timestamp': 1525255998747,
                 'topic': 'nyc_yellow_taxi_trip_data',
                 'partition': 0,
                 'offset': 0}],
            'offset': []
        }
        query = (
            "SELECT * FROM `nyc_yellow_taxi_trip_data` WHERE _vtype='AVRO' AND _ktype='BYTES' AND _sample=2 "
            "AND _sampleWindow=200 limit 1"
        )

        result = self.conn.SqlHandler(query)["messages"][0]["value"]
        assert recv["messages"][0]["value"] in result
