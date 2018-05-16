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

categories = ['', 'books', 'electronics', 'moviestv', 'cdsvinyl', 'clothingshoesjewelry', 'homekitchen', 'kindlestore', 'sportsoutdoors', 'cellphonesaccessories', 'healthpersonalcare', 'toysgames', 'videogames', 'toolshomeimprovement', 'beauty', 'appsforandroid', 'officeproducts', 'petsupplies', 'automotive', 'grocerygourmetfood', 'patiolawngarden', 'baby', 'digitalmusic', 'musicalinstruments', 'amazoninstantvideo']
category_dropdown = dcc.Dropdown(
    id = "category_dropdown",
    options = [{"label": c, "value": c} for c in categories],
    placeholder = "Select a category"
)

# all_products = q.getAllProducts()
# product_dropdown = dcc.Dropdown(
#     id = "product_dropdown",
#     options = [{"label": p[1], "value": p[0]} for p in all_products],
#     placeholder = "Select a product"
# )

app.layout = html.Div(children=[
    html.H1(children='Find the right users for a product'),

    html.Div(children='''
        Amazon User Review Data
    '''),
    html.Label("Products"),
    category_dropdown,
    dcc.Dropdown(id='product_dropdown', placeholder = "Select a product"),
    # product_dropdown,

    dcc.Graph(id='star_help_graph'),
 #   dcc.Graph(id='pos_sub_graph')

])

@app.callback(
    dash.dependencies.Output('product_dropdown', 'options'),
    [dash.dependencies.Input('category_dropdown', 'value')])
def getProductDropdown(cat):
    all_products = q.getAllProducts(cat)
    return [{"label": p[1], "value": p[0]} for p in all_products]

@app.callback(
    dash.dependencies.Output('star_help_graph', 'figure'),
    [dash.dependencies.Input('category_dropdown', 'value'),\
     dash.dependencies.Input('product_dropdown', 'value')])
def getStarHelpGraph(cat, productid):
    user_names = q.getRelevantUsers(productid, cat)
    user_data = q.getUsersData(user_names, cat)
    star = []
    helpful = []
    user_id = []
    for u in user_data:
        user_id.append(u[0])
        star.append(u[1])
        helpful.append(u[3]/u[2])

    return {
        'data': [
            go.Scatter(
                x = star,
                y = helpful,
                text = user_id,
                mode = 'markers',
                opacity = 0.85,
                marker = {
                    'size': 12
                }
            )
        ],
        'layout': go.Layout(
            font = {'size': 18},
            xaxis = {'title': 'Average Star Rating'},
            yaxis = {'title': 'Average Helpful Votes'},
            hovermode = 'closest'),

    }

#@app.callback(
#    dash.dependencies.Output('pos_sub_graph', 'figure'),
#    [dash.dependencies.Input('product_dropdown', 'value')])
#def getPosSubGraph(productid):
#    user_names = q.getRelevantUsers(productid)
#    user_data = q.getUsersData(user_names)
#    pos = []
#    sub = []
#    user_id = []
#    for u in user_data:
#        user_id.append(u[0])
#        pos.append(u[6]/u[7])
#        sub.append(u[-1])
#    return {
#        'data': [
#            go.Scatter(
#                x = pos,
#                y = sub,
#                text = user_id,
#                mode = 'markers'
#            )
#        ],
#        'layout': go.Layout(
#            xaxis = {'title': 'Positivity'},
#            yaxis = {'title': 'Subjectivity'}),
#    }


if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0")
