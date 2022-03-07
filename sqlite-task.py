# prefect_route: C:\Users\PC\AppData\Local\Programs\Python\Python39\Lib\site-packages\prefect

from cgitb import handler
from concurrent.futures.process import _chain_from_iterable_of_lists
from unittest import result
import requests, json
from prefect import task, Flow
from collections import namedtuple
from contextlib import closing
import sqlite3
import os
from prefect.tasks.database.sqlite import SQLiteScript
from prefect.schedules import IntervalSchedule
import datetime
from prefect.engine import signals
import prefect

from prefect.engine.results import LocalResult

os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin'

def alert_failed(obj, old_state, new_state):
    if new_state.is_failed():
        print('Ocurrió un fallo')

#setup
create_table = SQLiteScript(
    db='cfpbcomplaints.db',
    script='CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)'
)

# extraer
@task(cache_for=datetime.timedelta(days=1), state_handlers=[alert_failed], result=LocalResult(dir='F:\Documentos\Computación tolerante a fallas\prefect tutorial\prefect code'))
def get_complaint_data():
    r = requests.get('https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/', params={ 'size':10 })
    json_response = json.loads(r.text)
    # en caso de necesitar los logs
    logger = prefect.context.get('logger')
    logger.info('I actually requested this time!')
    return json_response['hits']['hits']

#transformar
@task(state_handlers=[alert_failed])
def parse_complaint_data(raw):
    #raise Exception
    # raise signals.FAIL
    # raise signals.SUCCESS
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
@task(state_handlers=[alert_failed])
def store_complaints(parsed):
    insert_cmd = 'INSERT INTO complaint VALUES (?, ?, ?, ?, ?)'

    with closing(sqlite3.connect('cfpbcomplaints.db')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(insert_cmd, parsed)
            conn.commit()

# ! establecer un intérvalo
# schedule = IntervalSchedule(interval=datetime.timedelta(minutes=1))

with Flow('mi flujo etl', state_handlers=[alert_failed]) as f:
    db_table = create_table()

    raw = get_complaint_data()
    parsed = parse_complaint_data(raw)
    populated_table = store_complaints(parsed)

    populated_table.set_upstream(db_table)

#f.visualize()
#f.run()
f.register(project_name='mis flujos')