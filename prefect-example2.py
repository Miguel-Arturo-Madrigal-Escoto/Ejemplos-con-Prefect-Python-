
""" json_response = json.loads(r.text)
# * print(json_response.keys())
# ? print(json_response['hits'].keys())
# ! print(json_response['hits']['hits'])

pprint.pprint(json_response['hits']['hits'][0]) """




from ast import With
from numpy import insert, product
import requests, json
from prefect import task, Flow
import os
os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin'

from collections import namedtuple
from contextlib import closing
import sqlite3
# extraer
@task
def get_complaint_data():
    r = requests.get('https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/', params={ 'size':10 })
    json_response = json.loads(r.text)
    
    return json_response['hits']['hits']

#transformar
@task
def parse_complaint_data(raw):
    complaints = []
    Complaint = namedtuple('Complaint', ['data_received', 'state', 'product', 'company', 'complaint_what_happened'])
    for row in raw:
        source = row.get('_source')
        this_complaint = Complaint( data_received = source.get('data_received'),
            state = source.get('state'),
            product = source.get('product'),
            company = source.get('company'),
            complaint_what_happened = source.get('complaint_what_happened')
        )
        complaints.append(this_complaint)
    return complaints


# load
@task
def store_complaints(parsed):
    create_script = 'CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)'
    insert_cmd = 'INSERT INTO complaint VALUES (?, ?, ?, ?, ?)'

    with closing(sqlite3.connect('cfpbcomplaints.db')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executescript(create_script)
            cursor.executemany(insert_cmd, parsed)
            conn.commit()

with Flow('mi flujo etl') as f:
    raw = get_complaint_data()
    parsed = parse_complaint_data(raw)
    store_complaints(parsed)

# f.visualize()
f.run()