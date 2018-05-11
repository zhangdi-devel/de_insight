#!/usr/bin/env python

from pg import DB
import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import getpass
import plotly


def getDB(user):
    db = DB(dbname=user, host='localhost', port=5432, user=user, passwd=user)
    return db


def getData(db, max_rows=20):
    st = "select * from pubg where period='lastHour' order by reported desc limit {}".format(max_rows)
    res = db.query(st)
    return res


def generate_table(data):
    cols = ['player', 'period', 'update time', 'kills',
            'deaths', 'reports', 'reported', 'tag']
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in cols])] +
        # Body
        [html.Tr([
            html.Td(data[row][col]) if col != 2 else data[row][col].ctime()
            for col in range(len(cols))
        ]) for row in range(len(data))]
    )


db = getDB(getpass.getuser())

app = dash.Dash()
app.layout = html.Div([
    html.H4(children='FairPlay Dashboard'),
    # generate_table(data)
    dcc.Graph(id='graph-fp'),
    dcc.Interval(
        id='interval-component',
        interval=1*1000,  # in milliseconds
        n_intervals=0
    )
], className="container")


@app.callback(
    Output('graph-fp', 'figure'),
    [Input('interval-component', 'n_intervals')])
def update_figure(n):
    data = getData(db).getresult()
    size = len(data)
    fig = plotly.tools.make_subplots(
        rows=4, cols=1,
        subplot_titles=['Kills', 'Deaths', 'Reportings', 'Be reported'],
        shared_xaxes=True
    )
    marker = {'color': ['#0074D9']*size}
    fig.append_trace({
        'x': [data[row][0] for row in range(size)],
        'y': [data[row][3] for row in range(size)],
        'type': 'bar',
        'marker': marker
    }, 1, 1)
    fig.append_trace({
        'x': [data[row][0] for row in range(size)],
        'y': [data[row][4] for row in range(size)],
        'type': 'bar',
        'marker': marker
    }, 2, 1)
    fig.append_trace({
        'x': [data[row][0] for row in range(size)],
        'y': [data[row][5] for row in range(size)],
        'type': 'bar',
        'marker': marker
    }, 3, 1)
    fig.append_trace({
        'x': [data[row][0] for row in range(size)],
        'y': [data[row][6] for row in range(size)],
        'type': 'bar',
        'marker': marker
    }, 4, 1)
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 1000
    fig['layout']['width'] = 600
    fig['layout']['margin'] = {
        'l': 40,
        'r': 40,
        't': 60,
        'b': 200
    }
    return fig


if __name__ == "__main__":
    data = getData(db)
    # res = [data[1][col] if col != 2 else data[1][col].ctime()
    #        for col in range(8)]  # for row in range(len(data))
    print(data)
    # print(generate_table(data))

    app.run_server(debug=True)
