import traceback
import time
import os
import sys
import uuid
import json
import argparse
from collections import deque
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

###############################################################################
# Below are all the static global variable initializations. Be warned that    #
# global variables can have weird interactions when multiple instances of the #
# app are running (See Dash documentatio for more information on this issue). # 
###############################################################################

DELAY = 1
MS = 1000 # milliseconds 
MB = 1e-6 # bytes in mb
COLOR_SCALE = os.environ.get('COLOR_SCALE', 11)
SERVICE_URL = os.environ.get('SERVICE_URL', '')
TOPIC = os.environ.get('TOPIC', '')
SUBSCRIPTION = os.environ.get('SUBSCRIPTION', '')
RETRIES = int(os.environ.get('RETRIES', 5))
SLEEP_TIME = float(os.environ.get('SLEEP_TIME', 5))
TIMEOUT = float(os.environ.get('TIMEOUT', .1))

# Set colorscale (global colors can conflict with multiple users)
# Note: It isnt worth converting the COLOR_GRAD to a deque unless you are doing
# 100+ poplefts and appends.
COLOR_RANGE = cl.scales['11']['div']['RdYlBu']
#COLOR_GRAD = cl.interp(COLOR_RANGE, COLOR_SCALE)
config = {
    "graph_bg": "#333436", 
    "graph_line": "#fff",
    "colors": {"": "#fff"},
    "color_opacity":.7,
    "line_color": "#fff",
    "line_width": .95
}

def arg_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('-d', '--debug', dest='debug',
        help="Run locally with fake data", action="store_true")

    return parser.parse_args()

# Get command line args
args = arg_parser()
DEBUG = args.debug # run locally for testing/UI work

###############################################################################
#  All utility functions for connecting are located below. These include      #
# Pulsar consumer code, parsing of Pulsar messages, data storage              #
# initialization and updating,and lastly dash callbacks for updating the app  #
###############################################################################

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

def consume():
    try:
        msg = consumer.receive(int(TIMEOUT*MS))
        if msg is None: return msg
        return json.loads(msg.data().decode())
    except Exception:
        if not DEBUG: print("Consumer polling timeout...")

def preload_data_store():
    store = {'graphData':{}, 'colorMaps':{}}
    store['graphData']['activeRanges'] = {'t1':50, 't2':25, 't3':25}
    store['graphData']['pseudoMsgCount'] = {'t1':50, 't2':20, 't3':100}
    store['graphData']['actualMsgCount'] = {'t1':50, 't2':30, 't3':100}
    store['colorMaps']['colorGrad'] = cl.interp(COLOR_RANGE, COLOR_SCALE)
    store['colorMaps']['consumer2color'] = {}

    # Assign colors
    for c in store['graphData']['activeRanges'].keys():
        assign_color(
            consumer=c, 
            consumer2color=store['colorMaps']['consumer2color'],
            color_grad=store['colorMaps']['colorGrad'])

    return store

def init_data_store():
    print("INITIALIZING GRAPHS...")
    store = {'graphData':{}, 'colorMaps':{}}
    store['graphData']['activeRanges'] = {}
    store['graphData']['pseudoMsgCount'] = {}
    store['graphData']['actualMsgCount'] = {}
    store['colorMaps']['colorGrad'] = cl.interp(COLOR_RANGE, COLOR_SCALE)
    store['colorMaps']['consumer2color'] = {}

    return store

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
    #print("DEBUG - Payload:{}".format(store))
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

def assign_color(consumer, consumer2color, color_grad):
    consumer2color[consumer] = color_grad[0]
    color_grad.append(color_grad.pop(0))

def update_data_store(payload, data_store):
    stats = parse_message(payload)
    graph_data = data_store['graphData']
    color_maps = data_store['colorMaps']
    left = set(stats['activeRanges']) # active consuners reported by newest message
    right = set(graph_data['actualMsgCount']) # consumers stored as active

    dropped_consumers = right - left
    new_consumers = left - right
    print('DEBUG - Disconnected', dropped_consumers)
    print('DEBUG - Connected', new_consumers)

    for c in dropped_consumers:
        graph_data['actualMsgCount'].pop(c, None)
        graph_data['pseudoMsgCount'].pop(c, None)
        color_maps['consumer2color'].pop(c, None)
        #color_maps['color2consumer'].pop(c_color, None)
        
    for c in new_consumers:
        # Initialize new consumer to show on graphs.
        graph_data['pseudoMsgCount'][c] = 0
        graph_data['actualMsgCount'][c] = 0
        assign_color(
            consumer=c, 
            consumer2color=color_maps['consumer2color'],
            color_grad=color_maps['colorGrad'])

    # Update active consumer information
    graph_data['activeRanges'] = compute_hash_size(stats['activeRanges'])
    graph_data['pseudoMsgCount'].update(stats['pseudoMsgCount'])
    graph_data['actualMsgCount'].update(stats['actualMsgCount'])
    #data_store['colorMaps'] = color_maps
    #data_store['graphData'] = graph_data

###############################################################################
# App intitialization begins here. This will create and start the Dash app    #
# using Flask. ALl Dash callbacks can also be found below. Callbacks inform   #
# the Dash app when to update each component of the UI.                       #
###############################################################################

#Initialize Dash (Flask server)
app = dash.Dash(__name__)

# Initialize graphs
init = init_data_store() if not DEBUG else preload_data_store()
figs={
    'big-graph': update_dual_bar(
        x_pseudo=list(init['graphData']['pseudoMsgCount'].keys()),
        y_pseudo=list(init['graphData']['pseudoMsgCount'].values()), 
        x_actual=list(init['graphData']['actualMsgCount'].keys()),
        y_actual=list(init['graphData']['actualMsgCount'].values()),
        config=config),
    'small-graph-1': update_pie(
        values=list(init['graphData']['activeRanges'].values()), 
        labels=list(init['graphData']['activeRanges'].keys()),
        config=config),
    'small-graph-2': update_pie(        
        values=list(init['graphData']['actualMsgCount'].values()),
        labels=list(init['graphData']['actualMsgCount'].keys()),
        config=config)
}

# Build dashboard via Dash with initialized graphs
app.layout = server_layout(app, figs)

# Connect to consumer
if not DEBUG: client, consumer = connect(TIMEOUT, RETRIES)

@app.callback(
    Output('data-store', 'data'),
    [Input('interval-comp', 'n_intervals')],
    [State('data-store', 'data')])
def dashboard(n, data_store):
    payload = consume()

    # Initialize data store with JSON dictionary format
    if data_store is None:
        data_store = init_data_store() if not DEBUG else preload_data_store()
    if payload is not None:
        update_data_store(payload, data_store) # update data store

    data_store_size = sys.getsizeof(data_store)
    print("DEBUG - Data store: {}".format(data_store))
    print("DEBUG - Data store size: {} bytes {:.6f} mb"
        .format(data_store_size, data_store_size*MB))

    return data_store

@app.callback(  
    [
        Output('big-graph', 'figure'), 
        Output('small-graph-1', 'figure'),
        Output('small-graph-2', 'figure')
    ],
    [Input('data-store', 'data')])
def update_graphs(data_store):
    graph_data = data_store['graphData']
    config['colors'] = data_store['colorMaps']['consumer2color']
    figs = update_all(graph_data, config)

    return figs

if __name__ == '__main__':
    try:
        # debug=True: Produces live updates when code is changed, 
        # which this script reruns multipple times thus bugging out client.
        app.run_server(debug=DEBUG, host='0.0.0.0', port=8050)
    except Exception:
        traceback.print_exc()
    finally:
        if not DEBUG: client.close()
        print("Shutting down...")