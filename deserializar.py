import cloudpickle

f = open('prefect-result-2022-03-07t00-55-41-359214-00-00', 'rb')
content = f.read()

print(cloudpickle.loads(content))