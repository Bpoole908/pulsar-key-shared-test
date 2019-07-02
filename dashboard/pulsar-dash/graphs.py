import numpy as np 
import dash
import plotly
import dash_core_components as dcc
import dash_html_components as html

def update_dual_bar(pseudo_x, pseudo_y, actual_x, actual_y, app_color):
    x_values = np.arange(0, len(pseudo_x))
    mark_color = list(app_color['colors'].values())
    group = list(app_color['colors'].keys())
    print(mark_color, group)

    layout = {
        'showlegend': False,
        'barmode':'group',
        'plot_bgcolor':app_color["graph_bg"],
        'paper_bgcolor':app_color["graph_bg"],
        'height': 800,
        'bargap': .6,
        'font':{"color": "#fff"},
        'xaxis':{
            'tickvals':x_values,
            'ticktext':pseudo_x,
            'gridcolor': 'rgb(112, 110, 110)'
        },
        'yaxis':{
            'gridcolor': 'rgb(112, 110, 110)'
        }
    }
    trace1 = {
        'x': x_values,
        'y': pseudo_y,
        'width': .2, 
        'name': 'Predicted Message Distribution',
        'hoverinfo': 'labels+values',
        'opacity': app_color['color_opacity'],
        'marker': {
            'color': mark_color
        },
        'group': group,
        'type': 'bar'
    }
    trace2 = {
        'x': x_values,
        'y': actual_y, 
        'width': .2,
        'name': 'Actual Message Distribution',
        'hoverinfo': 'labels+values',
        'opacity': app_color['color_opacity'],
        'marker': {
            'color': mark_color
        },
        'group': group,
        'type': 'bar'
    }
    data = [trace1, trace2]
    return {'data':data, 'layout':layout}

def update_pie(values, labels, app_color):
    mark_color = list(app_color['colors'].values())
    group = list(app_color['colors'].keys())
    print(values, labels, mark_color, group)
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
        'name': 'Active Hash Ranges',
        'hoverinfo': 'labels+values',
        'opacity': app_color['color_opacity'],
        'marker': {
            'colors': mark_color
        },
        'group': group,
        'type': 'pie'
    }
    data = [trace1]
    return {'data':data, 'layout':layout}

def update_all(graph_data, app_colors):
   
    msg_compare_fig = update_dual_bar(
        pseudo_x=list(graph_data['pseudoMsgCount'].keys()),
        pseudo_y=list(graph_data['pseudoMsgCount'].values()),
        actual_x=list(graph_data['actualMsgCount'].keys()),
        actual_y=list(graph_data['actualMsgCount'].values()),
        app_color=app_colors)
    hash_fig = update_pie(
        values=list(graph_data['activeRanges'].values()), 
        labels=list(graph_data['activeRanges'].keys()),
        app_color=app_colors)
    msg_dist_fig = update_pie(
        values=list(graph_data['actualMsgCount'].values()), 
        labels=list(graph_data['actualMsgCount'].keys()),
        app_color=app_colors)

    # Must be returned in order of Output!
    return msg_compare_fig, hash_fig, msg_dist_fig
