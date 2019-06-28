import dash
import plotly
import plotly.graph_objs as go
import dash_core_components as dcc
import dash_html_components as html

def server_layout(app, figs):
    return html.Div(
    children=[
        dcc.Store(id='data-store',storage_type='memory'),
        # header
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.H4("Puslar: Key Sharing Demo", className="app__header__title"),
                        html.P(
                            "This is a visualize rpersentation of Pulsar's key sharing subscription type.",
                            className="app__header__title--grey",
                        ),
                    ],
                    className="app__header__desc",
                ),
            ],
            className="app__header",
        ),
        html.Div(
            [
                html.Div(
                    children=[
                        html.Div(
                            [html.H6("Acutal vs Predicted Messages", className="graph__title")]
                        ),
                        dcc.Graph(
                            id="big-graph",
                            figure=figs['big-graph']
                        ),
                    ],
                    className="two-thirds column wind__speed__container",
                ),
                html.Div(
                    children=[
                        # hash range pie
                        html.Div(
                            children=[
                                html.Div(
                                    children=[
                                        html.H6(
                                            "Consumer Hash Ranges",
                                            className="graph__title",
                                        )
                                    ]
                                ),
                                dcc.Graph(
                                    id="small-graph-1",
                                    figure=figs['small-graph-1']
                                ),
                            ],
                            className="graph__container first",
                        ),
                        # wind direction
                        html.Div(
                            children=[
                                html.Div(
                                    [
                                        html.H6(
                                            "Message Distribution", className="graph__title"
                                        )
                                    ]
                                ),
                                dcc.Graph(
                                    id="small-graph-2",
                                    #figure=figs['small-graph-2']
                                ),
                            ],
                            className="graph__container second",
                        ),
                    ],
                    className="one-third column histogram__direction",
                ),
            ],
            className="app__content",
        ),
        dcc.Interval(
            id="interval-comp",
            interval=int(1000),
            n_intervals=0,
        ),
    ],
    className="app__container",
)