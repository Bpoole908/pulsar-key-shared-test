import traceback
import time
import os
import sys
import uuid
import json

import numpy as np 
import pandas as pd
import pulsar
import dash
import plotly
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
'''
    TODO:
        - Cache graph init (test when init is recalled with cache)
        - Track total memory usage
'''
# Static global varibales, DO NOT CHANGE!
DELAY = 1
MS = 1000 # milliseconds 
SERVICE_URL = os.environ.get('SERVICE_URL')
TOPIC = os.environ.get('TOPIC')
SUBSCRIPTION = os.environ.get('SUBSCRIPTION')
RETRIES = int(os.environ.get('RETRIES'))
SLEEP_TIME = float(os.environ.get('SLEEP_TIME'))
TIMEOUT = float(os.environ.get('TIMEOUT'))

def connect(timeout=5, retries=5):
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

def init_data_store():
     store = {}
     store['activeRanges'] = {'':0}
     store['pseudoMsgCount'] = {'':0}
     store['actualMsgCount'] = {'':0}

     return store

def update_graph(data_store):
    print('DATA_STORE {}'.format(data_store))
    # Initialize plotly graph 
    #fig = plotly.tools.make_subplots(rows=1, cols=1, vertical_spacing=0.2)
    #fig['layout']['margin'] = {'l': 30, 'r': 10, 'b': 30, 't': 10}
    #fig['layout']['legend'] = {'x': 0, 'y': 1, 'xanchor': 'left'}
    layout = {
        'title': 'Key Sharing Stats',
        'showlegend': False
    }
    # genrate colors for active consumer
    # generate colors for hist consumers
    pie = {
        'values': list(data_store['activeRanges'].values()), 
        'labels': list(data_store['activeRanges'].keys()),
        'name': 'Active Hash Ranges',
        'hoverinfo': 'labels+values',
        'type': 'pie',
        'domain': {
            'x': [0, .48],
            'y': [0, .49]
        },
    }
    bar_trace1 = {
        'values': list(data_store['pseudoMsgCount'].values()), 
        'labels': list(data_store['pseudoMsgCount'].keys()),
        'name': 'Active Hash Ranges',
        'hoverinfo': 'labels+values',
        'type': 'bar',
    }
    bar_trace2 = {
        'values': list(data_store['pseudoMsgCount'].values()), 
        'labels': list(data_store['pseudoMsgCount'].keys()),
        'name': 'Active Hash Ranges',
        'hoverinfo': 'labels+values',
        'type': 'bar',
    }
    bar = [bar_trace1, bar_trace2]
    return {
        'data' : [pie],
        'layout': layout
    }
    
def server_layout():
    fig = update_graph(init_data_store())
    return html.Div([
    dcc.Store(id='data-store',storage_type='memory'),
    html.H4('Key Sharing'),
    html.Div(id='dash-test'),
    dcc.Graph(figure=fig, id='live-norm-graph'),
    dcc.Interval(
        id='interval-comp',
        interval=DELAY*MS,
        n_intervals=0)
    ])

# Connect to consumer
client, consumer = connect(TIMEOUT, RETRIES)

# Start and initialize Dash (Flask server)
stylesheets = [' https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=stylesheets)
app.layout =  server_layout()

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

    return store

def update_data_store(payload, data_store):
    if data_store is None:
        stats = init_data_store()
    stats = parse_message(payload)
    
    # Active consumers simply replace data store elements while consumer
    # history appends to data store (memory growth needs to be watched).
    data_store['activeRanges'] = stats['activeRanges']
    data_store['pseudoMsgCount'] = stats['pseudoMsgCount']
    data_store['actualMsgCount'] = stats['actualMsgCount']

@app.callback(
    [Output('live-norm-graph', 'figure'), Output('data-store', 'data')],
    [Input('interval-comp', 'n_intervals')],
    [State('data-store', 'data')])
def dashboard(n, data_store):

    def set_output(output, data_store):
        output.append(update_graph(data_store))
        output.append(data_store)

    payload = consume()
    output = []

    if payload is not None:
        if data_store is not None:
            update_data_store(payload, data_store)
        else:
            data_store = parse_message(payload)
    else:
        if data_store is None:
            data_store = init_data_store()
            
    set_output(output, data_store)
    print("OUTPUT: {}".format(output[-1]))
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