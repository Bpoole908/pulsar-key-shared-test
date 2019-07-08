import dash
import plotly
import plotly.graph_objs as go
import dash_core_components as dcc
import dash_html_components as html

def server_layout(app, figs, interval=1):
    return html.Div(
    children=[
        dcc.Store(id='data-store',storage_type='memory'),
        # Title
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.H4("Puslar: Key Sharing Demo", className="app__header__title"),
                        html.P("""
                                This is app provides a visualization of 
                                Pulsar's key sharing subscription type.
                                """,
                            className="app__header__title--grey",
                        ),
                    ],
                    className="app__header__desc",
                ),
            ],
            className="app__header",
        ),
        html.Div(
            children=[
                html.Div(
                    children=[
                        # Message comparison
                        html.Div(
                            children=[
                                html.Div(
                                    children=[
                                        html.H6(
                                        "Acutal vs Predicted Messages", 
                                        className="graph__title"
                                        )
                                    ]
                                ),
                                dcc.Graph(
                                    id="big-graph-1",
                                    figure=figs['big-graph-1']
                                ),
                            ],
                            className="large__graph__container first",
                        ),
                        # Active hash ranges
                        html.Div(
                            children=[
                                html.Div(
                                    children=[

                                    ]
                                ),
                                #dcc.Graph(

                                #),
                            ],
                            className="large__graph__container second",
                        ),
                    ],
                    className="two-thirds column histogram__direction",
                ),
                html.Div(
                    children=[
                        # Message distribution   
                        html.Div(
                            children=[
                                html.Div(
                                    children=[
                                        html.H6(
                                            "Message Distribution",
                                            className="graph__title",
                                        )
                                    ]
                                ),
                                dcc.Graph(
                                    id="small-graph-2",
                                    figure=figs['small-graph-2']
                                ),
                            ],
                            className="small__graph__container first",
                        ),
                        # Active hash sizes
                        html.Div(
                            children=[
                                html.Div(
                                    children=[
                                        html.H6(
                                            "Consumer Hash Range Sizes",
                                            className="graph__title"
                                        )
                                    ]
                                ),
                                dcc.Graph(
                                    id="small-graph-1",
                                    figure=figs['small-graph-1']
                                ),
                            ],
                            className="small__graph__container second",
                        ),
                    ],
                    className="one-third column histogram__direction",
                ),
            ],
            className="app__content",
        ),
        dcc.Interval(
            id="interval-comp",
            interval=int(interval*1000),
            n_intervals=0,
        ),
    ],
    className="app__container",
)