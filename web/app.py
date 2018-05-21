#!/usr/bin/env python

from pg import DB
import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import getpass
import plotly
import pandas as pd
import json
import sys

columnNames = ['Player', 'period', 'update time', 'Kills',
               'Deaths', 'Reports', 'Be reported', 'Tag']


def getDB(host, user):
    db = DB(dbname=user, host=host, port=5432, user=user, passwd=user)
    return db


# db = getDB('localhost', getpass.getuser())
db = getDB('ec2-54-190-243-251.us-west-2.compute.amazonaws.com', 'ubuntu')


def getData(db, period, max_rows, sortCol):
    interval = '1 hour'
    if period == 'lastDay':
        interval = '1 day'
    elif period == 'lastMonth':
        interval = '1 month'
    st = ("select * from pubg where period='{0}' and time > ("
          "select max(time) from pubg where period='{0}') - interval '{1}'"
          "order by {2} desc limit {3}"
          .format(period, interval, sortCol, max_rows))
    res = db.query(st).getresult()
    data = pd.DataFrame.from_records(res, columns=columnNames)
    return data


app = dash.Dash()
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})
app.layout = html.Div([
    html.H4(children='FairPlay Dashboard'),
    # generate_table(data)

    html.Div([
        html.Div([
            html.Label('Sort by'),
            dcc.RadioItems(
                id='sort-col',
                options=[
                    {'label': ' Kills', 'value': 'kills'},
                    {'label': ' Deaths', 'value': 'deaths'},
                    {'label': ' Reports', 'value': 'reports'},
                    {'label': ' Be reported', 'value': 'reported'}
                ],
                value='reported'
            )
        ], className='two columns'),
        html.Div([
            html.Label('Data refreshing frequency'),
            dcc.RadioItems(
                id='refresh-freq',
                options=[
                    {'label': ' 1 second', 'value': '1'},
                    {'label': ' 5 seconds', 'value': '5'},
                    {'label': ' 1 minute', 'value': '60'},
                    {'label': ' very long', 'value': '3600'}
                ],
                value='5'
            )
        ], className='two columns'),
        html.Div([
            html.Label('Show players'),
            dcc.RadioItems(
                id='num-players',
                options=[
                    {'label': ' top 20', 'value': '20'},
                    {'label': ' top 40', 'value': '40'},
                    {'label': ' top 60', 'value': '60'},
                    {'label': ' top 80', 'value': '80'}
                ],
                value='60'
            )
        ], className='two columns'),
        html.Div([
            html.Label('Time window'),
            dcc.RadioItems(
                id='time-window',
                options=[
                    {'label': ' 1 hour', 'value': 'lastHour'},
                    {'label': ' 24 hours', 'value': 'lastDay'},
                    {'label': ' 30 days', 'value': 'lastMonth'}
                ],
                value='lastDay'
            )
        ], className='two columns')
    ], className='row'),
    html.Div([
        html.Div([
            dt.DataTable(
                id='table-fp',
                rows=[{}],
                row_selectable=True,
                sortable=True,
                selected_row_indices=[]
            )
        ], className='eight columns')
    ], className='row'),
    html.Div([
        dcc.Graph(id='graph-fp')
    ], className='row'),
    dcc.Interval(
        id='interval-component',
        interval=60000,  # in milliseconds
        n_intervals=0
    ),
    html.Div(id='data', style={'display': 'none'}),
    html.Div(id='conf', style={'display': 'none'})
])


@app.callback(
    Output('interval-component', 'interval'),
    [Input('refresh-freq', 'value')]
)
def update_interval(freq):
    return int(freq) * 1000


@app.callback(
    Output('conf', 'children'),
    [Input('num-players', 'value'),
     Input('time-window', 'value'),
     Input('sort-col', 'value')]
)
def update_conf(num, window, sort_col):
    conf = {
        'num': num,
        'window': window,
        'sortCol': sort_col
    }
    confJson = json.dumps(conf)
    return confJson


@app.callback(
    Output('data', 'children'),
    [Input('conf', 'children'),
     Input('interval-component', 'n_intervals')],
    [State('data', 'children')]
)
def update_data(confJson, n, old):
    conf = json.loads(confJson)
    try:
        df = getData(db, conf['window'], conf['num'], conf['sortCol'])
        return df.to_json(date_format='iso', orient='split')
    except Exception:
        return old


@app.callback(
    Output('table-fp', 'selected_row_indices'),
    [Input('graph-fp', 'clickData')],
    [State('table-fp', 'selected_row_indices')]
)
def update_selected_rows(clickData, selected_rows):
    if clickData:
        for point in clickData['points']:
            if point['pointNumber'] in selected_rows:
                selected_rows.remove(point['pointNumber'])
            else:
                selected_rows.append(point['pointNumber'])
    return selected_rows


@app.callback(
    Output('table-fp', 'rows'),
    [Input('data', 'children')],
    [State('conf', 'children')]
)
def update_table(dataJson, confJson):
    data = pd.read_json(dataJson, orient='split')
    df = data[['Player', 'Kills', 'Deaths', 'Reports', 'Be reported', 'Tag']]
    return df.to_dict('records')


@app.callback(
    Output('graph-fp', 'figure'),
    [Input('data', 'children')],
    [State('table-fp', 'selected_row_indices')]
)
def update_figure(dataJson, selected_rows):
    js = pd.read_json(dataJson, orient='split')
    data = js[['Player', 'Kills', 'Deaths', 'Reports', 'Be reported', 'Tag']]
    size = len(data)
    fig = plotly.tools.make_subplots(
        rows=4, cols=1,
        subplot_titles=['Kills', 'Deaths', 'Reportings', 'Be reported'],
        shared_xaxes=True,
        print_grid=False
    )
    marker = {'color': ['#0074D9']*size}
    for i in (selected_rows or []):
        marker['color'][i] = '#FF851B'
    fig.append_trace({
        'x': data['Player'],
        'y': data['Kills'],
        'type': 'bar',
        'marker': marker
    }, 1, 1)
    fig.append_trace({
        'x': data['Player'],
        'y': data['Deaths'],
        'type': 'bar',
        'marker': marker
    }, 2, 1)
    fig.append_trace({
        'x': data['Player'],
        'y': data['Reports'],
        'type': 'bar',
        'marker': marker
    }, 3, 1)
    fig.append_trace({
        'x': data['Player'],
        'y': data['Be reported'],
        'type': 'bar',
        'marker': marker
    }, 4, 1)
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 1000
    fig['layout']['width'] = 800
    fig['layout']['margin'] = {
        'l': 40,
        'r': 40,
        't': 60,
        'b': 200
    }
    return fig


if __name__ == "__main__":
    app.run_server(debug=True)
