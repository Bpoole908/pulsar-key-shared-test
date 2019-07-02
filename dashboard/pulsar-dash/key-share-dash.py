import traceback
import time
import os
import sys
import uuid
import json
from pdb import set_trace
from collections import OrderedDict   

import numpy as np 
import pandas as pd
import colorlover as cl
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
MB = 1e-6 # bytes in mb
COLOR_SCALE = 500
SERVICE_URL = os.environ.get('SERVICE_URL', '')
TOPIC = os.environ.get('TOPIC', '')
SUBSCRIPTION = os.environ.get('SUBSCRIPTION', '')
RETRIES = int(os.environ.get('RETRIES', 5))
SLEEP_TIME = float(os.environ.get('SLEEP_TIME', 5))
TIMEOUT = float(os.environ.get('TIMEOUT', .1))

# Set colorscale (global colors can conflict with multiple users)
color_range = cl.scales['9']['div']['Spectral']
color_gradient = cl.interp(color_range, COLOR_SCALE)

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
    store = {'graph_data':{}, 'color_maps':{}}
    store['graph_data']['activeRanges'] = {'t1':50, 't2':25, 't3':25}
    store['graph_data']['pseudoMsgCount'] = {'t1':50, 't2':20, 't3':100}
    store['graph_data']['actualMsgCount'] = {'t1':50, 't2':30, 't3':100}
    store['color_maps']['consumer2color'] = {'':''}
    store['color_maps']['color2consumer'] = {'':''}
    print(store)
    return store'''
def init_data_store():
    store = {'graph_data':{}, 'color_maps':{}}
    store['graph_data']['activeRanges'] = {}
    store['graph_data']['pseudoMsgCount'] = {}
    store['graph_data']['actualMsgCount'] = {}
    store['color_maps']['consumer2color'] = {}
    store['color_maps']['color2consumer'] = {}

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
    print("DEBUG - Payload:{}".format(store))
    return store

def compute_hash_size(ranges):
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

def assign_color(active_consumers, consumer, consumer2color, color2consumer):
    while True:
        idx = np.random.randint(0, COLOR_SCALE)
        if color_gradient[idx] not in color2consumer:
            consumer2color[consumer] = color_gradient[idx]
            color2consumer[color_gradient[idx]] = consumer
            break

def update_data_store(payload, data_store):
    stats = parse_message(payload)
    color_maps = data_store['color_maps']
    graph_data = data_store['graph_data']
    left = set(stats['activeRanges']) # active consuners reported by newest message
    right = set(graph_data['actualMsgCount']) # consumers stored as active

    dropped_consumers = right - left
    new_consumers = left - right
    print('DEBUG - Disconnected', dropped_consumers)
    print('DEBUG - Connected', new_consumers)

    for c in dropped_consumers:
        graph_data['actualMsgCount'].pop(c, None)
        graph_data['pseudoMsgCount'].pop(c, None)
        c_color =  color_maps['consumer2color'].pop(c, None)
        color_maps['color2consumer'].pop(c_color, None)
        
    for c in new_consumers:
        # Initialize new consumer to show on graphs.
        graph_data['pseudoMsgCount'][c] = 0
        graph_data['actualMsgCount'][c] = 0
        assign_color(
            active_consumers=stats['activeRanges'].keys(), 
            consumer=c,
            consumer2color=color_maps['consumer2color'],
            color2consumer=color_maps['color2consumer'])

    # Update active consumer information
    graph_data['activeRanges'] = compute_hash_size(stats['activeRanges'])
    graph_data['pseudoMsgCount'].update(stats['pseudoMsgCount'])
    graph_data['actualMsgCount'].update(stats['actualMsgCount'])
    data_store['color_maps'] = color_maps
    data_store['graph_data'] = graph_data
# Connect to consumer
client, consumer = connect(TIMEOUT, RETRIES)

#Initialize Dash (Flask server)
static_colors = {
    "graph_bg": "#333436", 
    "graph_line": "#fff",
    "colors": {"":"#fff"},
    "color_opacity":.7
}
app = dash.Dash(__name__)
# Initialize graphs
init = init_data_store()

figs={
    'big-graph': update_dual_bar(
        pseudo_x=list(init['graph_data']['pseudoMsgCount'].keys()),
        pseudo_y=list(init['graph_data']['pseudoMsgCount'].values()), 
        actual_x=list(init['graph_data']['actualMsgCount'].keys()),
        actual_y=list(init['graph_data']['actualMsgCount'].values()),
        app_color=static_colors),
    'small-graph-1': update_pie(
        values=list(init['graph_data']['activeRanges'].values()), 
        labels=list(init['graph_data']['activeRanges'].keys()),
        app_color=static_colors),
    'small-graph-2': update_pie(        
        values=list(init['graph_data']['actualMsgCount'].values()),
        labels=list(init['graph_data']['actualMsgCount'].keys()),
        app_color=static_colors)
}
# Build dashboard via Dash 
app.layout = server_layout(app, figs)

@app.callback(
    Output('data-store', 'data'),
    [Input('interval-comp', 'n_intervals')],
    [
        State('data-store', 'data'),
    ])
def dashboard(n, data_store):
    payload = consume()

    if payload is not None:
        if data_store is not None:
            update_data_store(payload, data_store) # update data store
        else:
            data_store = parse_message(payload) # load first payload into datastore
    else:
        if data_store is None:
            data_store = init_data_store() # initialize data store

    data_store_size = sys.getsizeof(data_store)
    print("DEBUG - Data store: {}".format(data_store))
    print("DEBUG - Data store space: {} bytes {:.6f} mb"
        .format(data_store_size, data_store_size*MB))
    return data_store

@app.callback(  
    [
        Output('big-graph', 'figure'), 
        Output('small-graph-1', 'figure'),
        Output('small-graph-2', 'figure')
    ],
    [
        Input('data-store', 'data'),
        #Input('color-map', 'data')
    ])
def update_graphs(data_store):
    print('RUNNING UPDATE GRAPH')
    graph_data = data_store['graph_data']
    consumer_colors = data_store['color_maps']['consumer2color']
    app_colors = {**static_colors, 'colors':consumer_colors}
    print(app_colors)
    figs = update_all(graph_data, app_colors)
    return figs

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