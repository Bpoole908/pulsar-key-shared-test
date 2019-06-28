import traceback

import numpy as np 
import pandas as pd
import dash
import uuid
import plotly
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
from layout import server_layout
# Static global
DELAY = 1
GEN_DATA_SIZE = 10
TIMEOUT = 60

figs = {}
# Dash initialization
stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app_color = {"graph_bg": "#333436", "graph_line": "#333436"}
# Starting Flask server
app = dash.Dash(__name__)
'''cache =  Cache(
    app.server, 
    config={
        'CACHE_TYPE': 'filesystem',
        'CACHE_THRESHOLD' : 5, # max number of users on app
        'CACHE_DIR': 'cache-directory' # Q: What is this?
    })'''

layout_bar = {
    'showlegend': False,
    'barmode':'group',
    'plot_bgcolorp':app_color["graph_bg"],
    'paper_bgcolor':app_color["graph_bg"]
}

bar_trace1 = {
    'x': ['t1', 't2', 't3'],
    'y': [50,60,20], 
    'name': 'test',
    'type': 'bar',
}

bar_trace2 = {
    'x': ['t1', 't2', 't2'],
    'y': [50,70,40], 
    'name': 'test2',
    'type': 'bar',
}
bar = [bar_trace1, bar_trace2]
figs['small-graph-2'] = {'data':bar, 'layout':layout_bar}


layout_scatter = {
    'showlegend': False,
    'plot_bgcolorp':app_color["graph_bg"],
    'paper_bgcolor':app_color["graph_bg"]
}

scatter = {
    'x': [0],
    'y': [0],
    'name': 'scatter-plot',
    'type': 'scatter'
}
figs['big-graph'] = {'data':[scatter], 'layout':layout_scatter}

pie = {
    'values': [25,25,50], 
    'labels':['t1', 't2', 't3'],
    'name': 'Active Hash Ranges',
    'hoverinfo': 'labels+values',
    'type': 'pie',
}
figs['small-graph-1'] = {'data':[pie], 'layout':layout_scatter}
'''def server_layout():
    #session_id = str(uuid.uuid4())
    print("RUNNING")
    return html.Div([
        #html.Div(session_id, id='session-id', style={'display': 'none'}),
        dcc.Store(id='store',storage_type='memory'),
        html.H4('Dashboard Test'),
        html.Div(children=[
             html.Div(
            [
                dcc.Graph(id='live-norm-graph'),
            ], 
            style={
                'width': '50%', 
                'display': 'inline-block',
                'vertical-align': 'left'
            }
        ),
        html.Div([
            dcc.Graph(figure=fig, id='static-graph')],
            style={
                'width': '50%', 
                'display': 'inline-block', 
                'vertical-align': 'right'
            }
        ),
      
        dcc.Interval(
            id='interval-comp',
            interval=DELAY*1000,
            n_intervals=0)
        ])
    ])'''

app.layout = server_layout(app, app_color, figs)

def normal_dist(x, mu=0, sigma=1):
    mu = np.full(x.shape, mu)
    sigma = np.full(x.shape, sigma)
    return (1/(np.sqrt(2*np.pi))*sigma) * np.exp(-(x - mu)**2/(2*sigma**2))

def query_data(n, low=-5, high=5):
    x = np.random.uniform(low, high, (n,1))
    y = normal_dist(x)
    coords = np.concatenate([x,y], 1)

    return coords

def add_data(data, new_data):
    if data is None:
        data = new_data
    elif len(data) >=  100:
        data[:-GEN_DATA_SIZE] = data[GEN_DATA_SIZE:]
        data[-GEN_DATA_SIZE:] = new_data
    else:
        data = np.concatenate([data,new_data], 0)

    return np.array(data)

@app.callback(
    [Output('big-graph', 'figure'), Output('store', 'data')],
    [Input('interval-comp', 'n_intervals')],
    [State('store', 'data')])
def update_graph(n, data):
    new_data = query_data(n=10)
    df = add_data(data, new_data)
    # Create the graph with subplots
    #fig = plotly.tools.make_subplots(rows=1, cols=1, vertical_spacing=0.2)
    #fig['layout']['margin'] = {'l': 30, 'r': 10, 'b': 30, 't': 10}
    #fig['layout']['legend'] = {'x': 0, 'y': 1, 'xanchor': 'left'}
    data = {
        'x': df[:, 0],
        'y': df[:, 1],
        'name': 'Random',
        'mode': 'markers',
        'type': 'scatter'
    }
    fig ={'data':[data], 'layout':layout_scatter}
    #fig = go.Figure(data=data, layout=layout_scatter)
    #print(type(fig), type(df), fig)
    return [fig, df]

if __name__ == '__main__':
    try:
        app.run_server(debug=True, host='0.0.0.0', port=8050)
    except Exception:
        traceback.print_exc()