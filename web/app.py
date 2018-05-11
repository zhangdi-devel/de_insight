#!/usr/bin/env python

from pg import DB
import dash
import dash_core_coponents as dcc
import dash_html_components as html

user = 'zhangdi'
db = DB(dbname=user, host='localhost', port=5432, user=user, passwd=user)

def get(db):
    res = db.query('select * from pubg order by reported desc limit 20')
    return res

def generate_table(data, max_rows=20):
    cols = ['player', 'update time', 'kills', 'deaths',
    'reports', 'reported', 'tag']
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in cols])] +
        # Body
        [html.Tr([
            html.Td()
        ])]
    )

if __name__ == "__main__":
    res = get(db)
    print(res)
