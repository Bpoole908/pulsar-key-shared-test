import traceback
import time
import os
import sys
import uuid
import json
from collections import OrderedDict   

import numpy as np 
import pandas as pd
import pulsar
import dash
import plotly
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from layout import server_layout
from graphs import update_dual_bar, update_pie, update_all
'''
    TODO:
        - Cache graph init (test when init is recalled with cache)
        - Track total memory usage
'''
# Static global varibales, DO NOT CHANGE!
DELAY = 1
MS = 1000 # milliseconds 
SERVICE_URL = os.environ.get('SERVICE_URL', '')
TOPIC = os.environ.get('TOPIC', '')
SUBSCRIPTION = os.environ.get('SUBSCRIPTION', '')
RETRIES = int(os.environ.get('RETRIES', 5))
SLEEP_TIME = float(os.environ.get('SLEEP_TIME', 5))
TIMEOUT = float(os.environ.get('TIMEOUT', .1))

def connect(timeout, retries):
    client = pulsar.Client(SERVICE_URL)
    for attempt in range(retries):
        try:
            print("CONNECTED - Topic: {}".format(TOPIC))
            return client, client.subscribe(TOPIC, SUBSCRIPTION)
        except Exception:
            print("CONNECTING: Attempt {}/{}".format(attempt, retries))
            time.sleep(SLEEP_TIME)
    print("Failed to connect")
    sys.exit(1)

'''def init_data_store():
     store = {}
     store['activeRanges'] = {'t1':100}
     store['pseudoMsgCount'] = {'t1':50, 't2':20}
     store['actualMsgCount'] = {'t1':50, 't2':30}

     return store'''
def init_data_store():
     store = {}
     store['activeRanges'] = {'':0}
     store['pseudoMsgCount'] = {'':0}
     store['actualMsgCount'] = {'':0}

     return store

def consume():
    try:
        msg = consumer.receive(int(TIMEOUT*MS))
        if msg is None: return msg
        return json.loads(msg.data().decode())
    except Exception:
        print("Consumer polling timeout...")

def parse_message(payload):
    store = {}
    # Extract dicitonary of conneceted consumers reprted with message
    store['activeRanges'] = payload['producer']['connected']
    # Extract what the pseudo stream predicted 
    pseudo = payload['producer']['pseudoConsumer']
    store['pseudoMsgCount'] = {pseudo['name']:pseudo['messageCount']}
    # Extract what the consumer actually reported
    consumer = payload['consumer']
    store['actualMsgCount'] = {consumer['name']:consumer['messageCount']}
    print(store)
    return store

def calc_range_sizes(ranges):
    range_size = {}
    ranges_sorted = sorted(ranges.items(), key=lambda kv: kv[1])

    for i, c in enumerate(ranges_sorted):
        key=c[0]
        if i == 0:
            range_size[key] = ranges[key] - 0
        else:
            range_size[key] = ranges[key] - ranges_sorted[i-1][1]

    print("DEBUG - Ranges: {}".format(ranges))
    print("DEBUG - Range sizes: {}".format(range_size))
    return range_size

def update_data_store(payload, data_store):
    stats = parse_message(payload)
    left = set(data_store['activeRanges'])
    right = set(data_store['actualMsgCount'])

    dropped_consumers = right - left
    new_consumers = left - right
    print('DEBUG - Disconnected', dropped_consumers)
    print('DEBUG - Connected', new_consumers)

    for c in dropped_consumers:
         data_store['actualMsgCount'].pop(c, None)
         data_store['pseudoMsgCount'].pop(c, None)
    for c in new_consumers:
        data_store['actualMsgCount'][c] = 0
    # Active consumers simply replace data store elements while consumer
    # history appends to data store (memory growth needs to be watched).
    data_store['activeRanges'] = calc_range_sizes(stats['activeRanges'])
    data_store['pseudoMsgCount'].update(stats['pseudoMsgCount'])
    data_store['actualMsgCount'].update(stats['actualMsgCount'])

# Connect to consumer
client, consumer = connect(TIMEOUT, RETRIES)

#Initialize Dash (Flask server)
app_color = {"graph_bg": "#333436", "graph_line": "#fff"}
app = dash.Dash(__name__)
# Initialize graphs
init = init_data_store()

figs={
    'big-graph' : update_dual_bar(
        pseudo_x=list(init['pseudoMsgCount'].keys()),
        pseudo_y=list(init['pseudoMsgCount'].values()), 
        actual_x=list(init['actualMsgCount'].keys()),
        actual_y=list(init['actualMsgCount'].values()),
        app_color=app_color),
    'small-graph-1' : update_pie(
        values=list(init['activeRanges'].values()), 
        labels=list(init['activeRanges'].keys()),
        app_color=app_color),
    'small-graph-2' : update_pie(        
        values=list(init['actualMsgCount'].values()),
        labels=list(init['actualMsgCount'].keys()),
        app_color=app_color)
}
print(figs)
# Build dashboard via Dash 
app.layout =  server_layout(app, figs)

@app.callback(
    [
        Output('big-graph', 'figure'), 
        Output('small-graph-1', 'figure'),
        Output('small-graph-2', 'figure'),
        Output('data-store', 'data')
    ],
    [Input('interval-comp', 'n_intervals')],
    [State('data-store', 'data')])
def dashboard(n, data_store):

    def set_output(output, data_store):
        # Update all graphs with new data and append to output
        [output.append(f) for f in update_all(data_store, app_color)]
        output.append(data_store)

    payload = consume()
    output = []

    if payload is not None:
        if data_store is not None:
            update_data_store(payload, data_store) # update data store
        else:
            data_store = parse_message(payload) # load first payload into datastore
    else:
        if data_store is None:
            data_store = init_data_store() # initialize data store
            
    set_output(output, data_store)
    print("DEBUG- Data store: {}".format(output[-1]))
    return output

if __name__ == '__main__':
    try:
        # debug=True: Produces live updates when code is changed, 
        # which this script reruns multipple times thus bugging out client.
        app.run_server(debug=False, host='0.0.0.0', port=8050)
    except Exception:
        traceback.print_exc()
    finally:
        client.close()
        print("Shutting down...")