import numpy as np 
import dash
import plotly
import dash_core_components as dcc
import dash_html_components as html

def update_dual_bar(x_pseudo, y_pseudo, x_actual, y_actual, config):
    x_values = np.arange(0, len(x_pseudo))
    mark_color = list(config['colors'].values())
    group = list(config['colors'].keys())

    layout = {
        'showlegend': False,
        'barmode':'group',
        'plot_bgcolor':config["graph_bg"],
        'paper_bgcolor':config["graph_bg"],
        #'height': 800,
        'bargap': .6,
        'font':{"color": "#fff"},
        'xaxis':{
            'tickvals':x_values,
            'ticktext':x_pseudo,
            #'gridcolor': 'rgb(112, 110, 110)'
        },
        'yaxis':{
            'gridcolor': 'rgb(112, 110, 110)'
        }
    }
    # Data for pseudo messages received
    trace1 = {
        'x': x_values,
        'y': y_pseudo,
        'width': .2, 
        'name': 'Predicted Message Distribution',
        'hoverinfo': 'labels+values',
        'opacity': config['color_opacity'],
        'marker': {
            'line':{
                'color': config['line_color'],
                'width': config['line_width'],
            },
            'color': mark_color
        },
        'group': group,
        'type': 'bar'
    }
    # Data for actual messages received
    trace2 = {
        'x': x_values,
        'y': y_actual, 
        'width': .2,
        'name': 'Actual Message Distribution',
        'hoverinfo': 'labels+values',
        'opacity': config['color_opacity'],
        'marker': {
            'line':{
                'color': config['line_color'],
                'width': config['line_width'],
            },
            'color': mark_color
        },
        'group': group,
        'type': 'bar'
    }
    data = [trace1, trace2]
    return {'data':data, 'layout':layout}

def update_pie(values, labels, config):
    mark_color = list(config['colors'].values())
    group = list(config['colors'].keys())

    layout = {
        'showlegend': True,
        'plot_bgcolor':config["graph_bg"],
        'paper_bgcolor':config["graph_bg"],
        'autosize': True,
        'font':{"color": "#fff"}
    }
    trace1 = {
        'values': values, 
        'labels': labels,
        'name': 'Active Hash Ranges',
        'hoverinfo': 'labels+values',
        'opacity': config['color_opacity'],
        'marker': {
            'line':{
                'color': config['line_color'],
                'width': config['line_width'],
            },
            'colors': mark_color
        },
        'textfont': {
            'color': '#fff'
        },
        'group': group,
        'type': 'pie'
    }
    data = [trace1]
    return {'data':data, 'layout':layout}

def update_gantt(ranges, labels,):
    
    pass

def update_all(graph_data, config):
    # All dictionaries in graph_data have the same key. It should be noted
    # that the order of the keys varries. To keep same order use only one list
    # of keys (here actualMsgCount key is choosen).
    key = list(graph_data['actualMsgCount'].keys())
    actual_msgs = list(graph_data['actualMsgCount'].values())
    pseudo_msgs = list(graph_data['pseudoMsgCount'].values())
    active_ranges = list(graph_data['activeRanges'].values())

    msg_compare_fig = update_dual_bar(
        x_pseudo=key,
        y_pseudo=pseudo_msgs,
        x_actual=key,
        y_actual= actual_msgs,
        config=config)
    hash_fig = update_pie(
        values=active_ranges, 
        labels=key,
        config=config)
    msg_dist_fig = update_pie(
        values= actual_msgs, 
        labels=key,
        config=config)

    # Must be returned in order of Output!
    return [msg_compare_fig, hash_fig, msg_dist_fig]
