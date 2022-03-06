from prefect import task, Flow
import os

os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin'

@task
def hello_world() -> str:
    print('Hola mundo')
    return 'Hello Prefect'

@task
def prefect_says(s: str) -> None:
    print(s)

with Flow('Mi primer flujo') as f:
    r = hello_world()
    s2 = prefect_says(r)

f.visualize()
# f.run()