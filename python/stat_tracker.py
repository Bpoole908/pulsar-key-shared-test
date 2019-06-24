import json
import sys
import os
import time

import pulsar

TIME_UNIT = 1000 # milliseconds 

SERVICE_URL = os.environ.get('SERVICE_URL')
TOPIC = os.environ.get('TOPIC')
SUBSCRIPTION = os.environ.get('SUBSCRIPTION')
TIME_OUT = os.environ.get('TIME_OUT')
BEFORE_START = float(os.environ.get('BEFORE_START'))/TIME_UNIT

class Collector:
    def __init__(self, consumer):
        self.consumer = consumer

    @staticmethod
    def connect(topic, subscription, timeout):
        while True:
            try:
                return client.subscribe(TOPIC, SUBSCRIPTION)
            except Exception:
                time.sleep(timeout)

    def consume(self):

        while True:
            msg = self.consumer.receive(TIME_OUT)
            payload = json.loads(msg.data().decode())
            if msg is None: continue
            print("DEBUG - Message: {}".format(payload))

            self.consumer.acknowledge(msg)

if __name__ == '__main__':
    client = pulsar.Client(SERVICE_URL)
    print("Sleeping for {}".format(BEFORE_START))   
    consumer = Collector.connect(TOPIC, SUBSCRIPTION, 1)
    print("CONNECTED TO {}".format(TOPIC))
    collector = Collector(consumer)
    collector.consume()
    