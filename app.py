# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html

from flask import Flask
import plotly.graph_objs as go

import psycopg2

import query as q

server = Flask(__name__)
app = dash.Dash(__name__, server = server)

all_products = q.getAllProducts()
product_dropdown = dcc.Dropdown(
    id = "product_dropdown",
    options = [{"label": p[0], "value": p[0]} for p in all_products],
    placeholder = "Select a product"
)

app.layout = html.Div(children=[
    html.H1(children='Find the right users for a product'),

    html.Div(children='''
        Amazon User Review Data
    '''),
    html.Label("Products"),
    product_dropdown,

    dcc.Graph(id='user_graph')

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

    # dcc.Graph(
    #     id='scatter-graph',
    #     figure={
    #         'data': [
    #             go.Scatter(
    #                 x = pos,
    #                 y = net_helpfulness,
    #                 text = user_id,
    #                 mode = 'markers'
    #             )
    #             # {'x': list(range(5)), 'y': stars, 'type': 'bar', 'name': 'Stars'},
    #             # {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
    #         ],
    #         'layout': go.Layout(
    #             xaxis = {'title': 'Positivity'},
    #             yaxis = {'title': 'Helpful Votes'})
    #     }
    # ),

    # dcc.Graph(
    #     id='star-graph',
    #     figure={
    #         'data': [
    #             {'x': list(range(10)), 'y': stars, 'type': 'bar', 'name': 'Stars'},
    #             # {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
    #         ],
    #         'layout': {
    #             'title': 'Users Average Star Ratings'
    #         }
    #     }
    # ),

    # dcc.Graph(
    #     id='helpfulness-graph',
    #     figure={
    #         'data': [
    #             {'x': list(range(10)), 'y': helpfulness, 'type': 'bar', 'name': 'Helpfulness'},
    #             # {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
    #         ],
    #         'layout': {
    #             'title': 'Users Helpful Votes'
    #         }
    #     }
    # ),

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

@app.callback(
    dash.dependencies.Output('user_graph', 'figure'),
    [dash.dependencies.Input('product_dropdown', 'value')])
def update_graph(productid):
    user_names = q.getRelevantUsers(productid)
    user_data = q.getUsersData(user_names)

    pos = []
    helpful = []
    user_id = []
    for u in user_data:
        user_id.append(u[0])
        pos.append(u[6]/u[7])
        helpful.append(u[3]/u[2])

    return {
        'data': [
            go.Scatter(
                x = pos,
                y = helpful,
                text = user_id,
                mode = 'markers'
            )
        ],
        'layout': go.Layout(
            xaxis = {'title': 'Positivity'},
            yaxis = {'title': 'Helpful'}),
    }



if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0")
