import json
import sys
import os
import time
import traceback
from collections import deque

import pulsar

TIME_UNIT = 1000 # milliseconds 

SERVICE_URL = os.environ.get('SERVICE_URL')
TOPIC = os.environ.get('TOPIC')
SUBSCRIPTION = os.environ.get('SUBSCRIPTION')
TIME_OUT = os.environ.get('TIME_OUT')
SLEEP_TIME = float(os.environ.get('SLEEP_TIME'))/TIME_UNIT

class Collector:
    def __init__(self, consumer, maxlen=100):
        self.consumer = consumer
        self.__msg_store = deque(maxlen=maxlen)
        self.hist_pseudo = {}
        self.hist_actual = {}
        self.hist_ranges = {}

    @staticmethod
    def connect(service, topic, subscription, timeout):
        client = pulsar.Client(SERVICE_URL)
        while True:
            try:
                return client, client.subscribe(TOPIC, SUBSCRIPTION)
            except Exception:
                time.sleep(timeout)

    def consume(self):
        while True:
            msg = self.consumer.receive(TIME_OUT)
            payload = json.loads(msg.data().decode())

            if msg is None: continue
            #print("DEBUG - Message: {}".format(payload))
            
            self.__msg_store.append(payload)
            self.parse_message(payload)
            self.consumer.acknowledge(msg)
            
            active_pseudo = self.get_connected(self.hist_pseudo, self.connected)
            active_actual = self.get_connected(self.hist_actual, self.connected)
            print("History: \n\tPseudo:{} \n\tActaul:{} \n\tRanges:{}"
                .format(self.hist_pseudo, self.hist_actual, self.hist_ranges))
            print("Active: \n\tPseudo:{} \n\tActaul:{} \n\tRanges:{}"
                .format(active_pseudo ,active_actual, self.connected)) 

    def parse_message(self, msg):
        self.connected = msg['producer']['connected']
        pseudo = msg['producer']['pseudoConsumer']
        consumer = msg['consumer']

        # Track ALL consumers who were once connected and iterate messages 
        # received predicted for PSEUDO consumers.
        if pseudo['name'] in self.hist_pseudo:
            self.hist_pseudo[pseudo['name']] += 1
        else:
            self.hist_pseudo[pseudo['name']] = 1

        # Track ALL consumers who were once connected and iterate messages 
        # received of ACTUAL consumers.
        if consumer['name'] in self.hist_actual:
            self.hist_actual[consumer['name']] += 1
        else:
            self.hist_actual[consumer['name']] = 1
            
        # Get consumers that are not in hist_ranges but are in active_ranges
        if len(self.hist_ranges) != 0: 
            new_consumers = self.connected.keys() - self.hist_ranges.keys()
            # add difference to hist_ranges (newly connected consumers)
            for c in new_consumers:
                 self.hist_ranges[c] = self.connected[c]
        else:
            self.hist_ranges = self.connected
    
    def get_connected(self, tracker, connected):
       return {c: tracker.get(c, 0) for c in connected.keys()}

if __name__ == '__main__':
    client, consumer = Collector.connect(SERVICE_URL, TOPIC, SUBSCRIPTION, 
        SLEEP_TIME)
    print("CONNECTED TO {}".format(TOPIC))
    try:
        collector = Collector(consumer)
        collector.consume()
    except Exception as e:
        print(traceback.format_exc())
        client.close()
