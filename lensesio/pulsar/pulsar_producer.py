import pulsar

class SetupPulsar():
    def __init__(self, host):
        self.pulsar_host = host

    def CreatePulsarClient(self):
        self.client = pulsar.Client(
            self.pulsar_host
        )

    def DeletePulsarProducer(self):
        self.client.close()

    def StartPulsarProducer(self, topic):
        self.pulsar_topic = topic
        self.pulsar_producer = self.client.create_producer(
            self.pulsar_topic
        )

    def PulsarProduce(self, payload):
        self.pulsar_producer.send(
            (payload).encode('utf-8')
        )