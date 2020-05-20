try:
    import pulsar
except ImportError:
    pass 


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
        self.pulsar_producer = self.pulsar_client.create_producer(
            topic
        )

    def StopPulsarProducer(self):
        self.pulsar_producer.flush()
        self.pulsar_producer.close()

    def PulsarProduce(self, payload, enc=None):
        if enc:
            self.pulsar_producer.send(
                payload.encode(enc)
            )
        else:
            self.pulsar_producer.send(
                payload
            )

    def StartPulsarConsumer(self, topic, subscription_name):
        self.pulsar_consumer = self.pulsar_client.subscribe(
            topic,
            subscription_name=subscription_name
        )

    def StopPulsarConsumer(self):
        self.pulsar_consumer.close()

    def PulsarConsume(self):
        msg = self.pulsar_consumer.receive()
        self.pulsar_consumer.acknowledge(msg)
        return msg.data()

