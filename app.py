# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html

from flask import Flask
import plotly.graph_objs as go

server = Flask(__name__)
app = dash.Dash(__name__, server = server)

from cassandra.cluster import Cluster

CASSANDRA_SERVER    = ['54.245.66.232', '34.214.245.150', '54.218.181.48', '54.71.237.54', '54.190.226.253', '35.165.118.115', '52.11.177.167', '34.215.123.166']
CASSANDRA_NAMESPACE = "amazonreviews"

cluster = Cluster(CASSANDRA_SERVER)
session = cluster.connect()

session.execute("USE " + CASSANDRA_NAMESPACE)

rows = session.execute('SELECT * FROM userdata')

stars = []
helpfulness = []
unhelpfulness = []
for i in range(10):
    stars.append(rows[i].avg_star)

for i in range(10):
    helpfulness.append(rows[i].helpful)

# for i in range(10):
    # unhelpfulness.append(rows[i].unhelpful)

pos = []
net_helpfulness = []
user_id = []
for i in range(1000):
    pos.append(rows[i].pos/rows[i].pos_review_count)
    net_helpfulness.append(rows[i].helpful)
    user_id.append(rows[i].reviewerid)


app.layout = html.Div(children=[
    html.H1(children='Find the right users for a product'),

    html.Div(children='''
        Amazon User Review Data
    '''),

    # dcc.Graph(
    #     id='example-graph',
    #     figure={
    #         'data': [
    #             {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
    #             # {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
    #         ],
    #         'layout': {
    #             'title': 'Dash Data Visualization'
    #         }
    #     }
    # )

    dcc.Graph(
        id='scatter-graph',
        figure={
            'data': [
                go.Scatter(
                    x = pos,
                    y = net_helpfulness,
                    text = user_id,
                    mode = 'markers'
                )
                # {'x': list(range(5)), 'y': stars, 'type': 'bar', 'name': 'Stars'},
                # {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
            ],
            'layout': go.Layout(
                xaxis = {'title': 'Positivity'},
                yaxis = {'title': 'Helpful Votes'})
        }
    ),

    dcc.Graph(
        id='star-graph',
        figure={
            'data': [
                {'x': list(range(10)), 'y': stars, 'type': 'bar', 'name': 'Stars'},
                # {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
            ],
            'layout': {
                'title': 'Users Average Star Ratings'
            }
        }
    ),

    dcc.Graph(
        id='helpfulness-graph',
        figure={
            'data': [
                {'x': list(range(10)), 'y': helpfulness, 'type': 'bar', 'name': 'Helpfulness'},
                # {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
            ],
            'layout': {
                'title': 'Users Helpful Votes'
            }
        }
    ),

    # dcc.Graph(
    #     id='unhelpfulness-graph',
    #     figure={
    #         'data': [
    #             {'x': list(range(5)), 'y': unhelpfulness, 'type': 'bar', 'name': 'Unhelpfulness'},
    #             # {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
    #         ],
    #         'layout': {
    #             'title': 'Users Unhelpful Votes'
    #         }
    #     }
    # )
])

if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0")
