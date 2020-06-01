try:
    import pulsar
    from pulsar.schema import *
except ImportError:
    pass

from threading import Thread, enumerate, RLock
from sys import exc_info
import queue


class SetupPulsar():

    def __init__(self, active_threads, host, **kwargs):
        self.pulsar_host = host
        self.pulsar_client_opts = kwargs
        self.pulsar_active_threads = active_threads
        self.SetupPulsarClient()

    def SetupPulsarClient(self):
        self.pulsar_client = pulsar.Client(
            self.pulsar_host,
            **self.pulsar_client_opts
        )

    def ClosePulsarClient(self):
        self.pulsar_client.close()

    def StartPulsarProducer(self, topic, **kwargs):
        self.pulsar_producer = self.pulsar_client.create_producer(
            topic,
            **kwargs
        )

    def StopPulsarProducer(self):
        self.pulsar_producer.flush()
        self.pulsar_producer.close()

    def PulsarProduce(self, payload, **kwargs):
        self.pulsar_producer.send(
            payload,
            **kwargs
        )

    def StartPulsarConsumer(self, topic, subscription_name, **kwargs):
        self.pulsar_consumer = self.pulsar_client.subscribe(
            topic,
            subscription_name=subscription_name,
            **kwargs
        )

    def StopPulsarConsumer(self):
        self.pulsar_consumer.close()

    def stop_pc_thread(self, t, stop=False):
        pcthread = self.sub_active_threads['pulsar_consumer'][t]['thread']
        if not pcthread.is_alive():
            self.sub_active_threads['pulsar_consumer'][t]['state'] = False
            return self.pc_state_report(t)

        if stop and self.sub_active_threads['pulsar_consumer'][t]['state']:
            print("Pulsar Consumer Thread has been marked for shutdown")
            self.sub_active_threads['pulsar_consumer'][t]['state'] = False
            self.pc_running_state = False

        return self.pc_state_report(t)
    
    def pc_state_report(self, t):
        pcthread = self.pulsar_active_threads['pulsar_consumer'][t]['thread']
        if self.pulsar_active_threads['pulsar_consumer'][t]['state'] and pcthread.is_alive():
            msg = "Pulsar Consumer Thread is running"
        elif not self.pulsar_active_threads['pulsar_consumer'][t]['state'] and pcthread.is_alive():
            msg = "Waiting for thread to shutdown"
        else:
            msg = "Pulsar Consumer Thread has closed"

        return msg, self.pulsar_active_threads['pulsar_consumer'][t]['state']

    def consume_pc_queue(self, t):
        try:
            data = self.pulsar_active_threads['pulsar_consumer'][t]['consumerQue'].get(block=False)
            return data, True
        except queue.Empty:
            return "Empty Queue", False

    def LoopPC(self, consumerQue, t, **kwargs):
        def update_thread_state(msg, state=True):
            self.pulsar_active_threads['thread_lock'].acquire()
            self.pulsar_active_threads['pulsar_consumer'][t]['state'] = state
            self.pulsar_active_threads['pulsar_consumer'][t]['state_info'] = msg
            self.pulsar_active_threads['thread_lock'].release()

        def update_subscribe_metadata():
            self.sub_active_threads['thread_lock'].acquire()
            self.pulsar_active_threads['pulsar_consumer'][t]['recordsFetched'] += 1 
            self.sub_active_threads['thread_lock'].release()

        self.pc_running_state = True
        update_thread_state("Consuming")
        while self.pc_running_state:
            try:
                pulsarConsume = self.ExecPulsarConsumer(**kwargs)
                consumerQue.put(pulsarConsume)
                update_subscribe_metadata()
            except:
                update_thread_state(exc_info(), False)
                raise

        update_thread_state("Consumer has stopped", False)

    def PulsarConsume(self, spawn_thread=None, **kwargs):
        if spawn_thread:
            self.pulsar_active_threads['thread_lock'].acquire()
            self.pulsar_active_threads['pulsar_consumer']['t'] += 1
            t = self.pulsar_active_threads['pulsar_consumer']['t']
            self.pulsar_active_threads['thread_lock'].release()

            consumerQue=queue.Queue()
            self.new_pc = Thread(
                target=self.LoopPC,
                args=(
                    consumerQue,
                    t,
                ),
                kwargs=kwargs,
                daemon=False
            )

            self.pulsar_active_threads['thread_lock'].acquire()
            self.pulsar_active_threads['pulsar_consumer'][t] = {
                'consumerQue': consumerQue,
                "state": False,
                "state_info": None,
                "thread": self.new_pc,
                "recordsFetched": 0
            }

            self.new_pc.start()
            self.pulsar_active_threads['thread_lock'].release()

            print(
                "Pulsar Consumer Thread -\t with PULSAR_CONSUMER_ID: %s has been started\n" % t,
                "You may find thread info at callers_name.active_threads object\n",
                "To access the thread's data, use the thread's queue:\n"
                "\tcallers_name.active_theads['pulsar_consumer'][PULSAR_CONSUMER_ID][consumerQue]\n",
                "Example how to get data from a thread's queue with soft lock:\n",
                "\tcallers_name.active_theads['pulsar_consumer'][PULSAR_CONSUMER_ID]['consumerQue'].get(block=True, timeout=5)\n"
            )
        elif spawn_thread is None:
            pulsarConsume = self.ExecPulsarConsumer(**kwargs)
            return pulsarConsume

    def ExecPulsarConsumer(self, **kwargs):
        msg = self.pulsar_consumer.receive(**kwargs)
        self.pulsar_consumer.acknowledge(msg)
        return msg.data()
