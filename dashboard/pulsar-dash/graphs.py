import numpy as np 
import dash
import plotly
import dash_core_components as dcc
import dash_html_components as html

def update_dual_bar(pseudo_x, pseudo_y, actual_x, actual_y, app_color):
    layout = {
        'showlegend': False,
        'barmode':'group',
        'plot_bgcolor':app_color["graph_bg"],
        'paper_bgcolor':app_color["graph_bg"],
        'height': 800,
        'bargap': .6,
        'font':{"color": "#fff"}
    }
    trace1 = {
        'x': pseudo_x,
        'y': pseudo_y,
        'width': .2, 
        'name': 'Predicted Message Distribution',
        'hoverinfo': 'labels+values',
        'type': 'bar'
    }
    trace2 = {
        'x': actual_x,
        'y': actual_y, 
        'width': .2,
        'name': 'Actual Message Distribution',
        'hoverinfo': 'labels+values',
        'type': 'bar'
    }
    data = [trace1, trace2]
    return {'data':data, 'layout':layout}

def update_pie(values, labels, app_color):   
    layout = {
        'showlegend': True,
        'plot_bgcolor':app_color["graph_bg"],
        'paper_bgcolor':app_color["graph_bg"],
        'autosize': True,
        'font':{"color": "#fff"}
    }
    trace1 = {
        'values': values, 
        'labels': labels,
        #'opacity':0.8,  
        'name': 'Active Hash Ranges',
        'hoverinfo': 'labels+values',
        'type': 'pie'
    }
    data = [trace1]
    return {'data':data, 'layout':layout}

def update_all(data_store, app_color):
    msg_compare_fig = update_dual_bar(
        pseudo_x=list(data_store['pseudoMsgCount'].keys()),
        pseudo_y=list(data_store['pseudoMsgCount'].values()),
        actual_x=list(data_store['actualMsgCount'].keys()),
        actual_y=list(data_store['actualMsgCount'].values()),
        app_color=app_color)
    hash_fig = update_pie(
        values=list(data_store['activeRanges'].values()), 
        labels=list(data_store['activeRanges'].keys()),
        app_color=app_color)
    msg_dist_fig = update_pie(
        values=list(data_store['actualMsgCount'].values()), 
        labels=list(data_store['actualMsgCount'].keys()),
        app_color=app_color)

    # Must be returned in order of Output!
    return msg_compare_fig, hash_fig, msg_dist_fig
