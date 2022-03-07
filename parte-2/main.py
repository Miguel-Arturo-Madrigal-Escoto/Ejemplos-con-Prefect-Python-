from prefect import task, Flow
import prefect
from MySQLTask import MySQLTask
import os
import hashlib

os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin'

@task
def start_connection(db: str, user: str, password: str, host: str):
    return MySQLTask(db, user, password, host)

@task
def get_user_data() -> tuple:
    username = input('-> Nombre: ')
    email = input('-> Correo: ')
    password = input('-> Contraseña: ')
    password = hashlib.new('sha256', bytes(password, 'utf-8')).hexdigest() 
    return (username, email, password)

@task
def format_user(data: tuple) -> dict:
    user = {}
    user['username'] = data[0]
    user['email'] = data[1]
    user['password'] = hashlib.new('sha256', bytes(data[2], 'utf-8')).hexdigest()
    return user


@task
def store_user(user: dict, conn: MySQLTask) -> None:
    query = 'INSERT INTO usuario(username, email, password) VALUES(%s, %s, %s)'
    params = (user['username'], user['email'], user['password'])
    return conn.run(query, params)


with Flow('Parte 2 - Ejemplo') as flow:
    conn = start_connection(db='ejemplo_mysql', user='root', password='', host='127.0.0.1')

    data = get_user_data()
    user = format_user(data)

    save = store_user(user, conn)
    save.set_upstream(conn)

    if save != 0:
        logger = prefect.context.get('logger')
        logger.info('Usuario guardado en la bd!')

#flow.visualize()
flow.run()