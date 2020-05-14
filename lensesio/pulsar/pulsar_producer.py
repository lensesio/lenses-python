import pulsar

class SetupPulsar():
    def __init__(self, host):
        self.pulsar_host = host
        self.SetupPulsarClient()

    def SetupPulsarClient(self):
        self.pulsar_client = pulsar.Client(
            self.pulsar_host
        )

    def ClosePulsarClient(self):
        self.pulsar_client.close()

    def StartPulsarProducer(self, topic):
        self.pulsar_topic = topic
        self.pulsar_producer = self.pulsar_client.create_producer(
            self.pulsar_topic
        )

    def StopPulsarProducer(self):
        self.pulsar_producer.flush()
        self.pulsar_producer.close()
        self.ClosePulsarClient()

    def PulsarProduce(self, payload, enc=None):
        if enc:
            self.pulsar_producer.send(
                payload.encode(enc)
            )
        else:
            self.pulsar_producer.send(
                payload
            )
