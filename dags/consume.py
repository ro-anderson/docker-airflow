from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import urllib.request
import json
import numpy as np

def consume():
	url = 'http://flask-nginx/prediction/api/v1.0/predict'

	n_sample = 10
	n_feats = 5
	X = np.random.random(n_sample * n_feats).reshape(n_sample, n_feats)
	feat_names = ['feat_' + str(i) for i in range(n_feats)]

	feats_data = []
	for r in X:
		feats_data.append(dict(zip(feat_names, r)))


	data = {'features': feats_data}
	data = json.dumps(data)
	data = data.encode('utf-8')

	req = urllib.request.Request(url)
	req.add_header('Content-Type', 'application/json; charset=utf-8')
	req.add_header('Content-Length', len(data))

	response = urllib.request.urlopen(req, data)
	response = response.read().decode('utf-8')
	response = json.loads(response)
	print('Probabilidades:')
	print(response)


with DAG('consume_api', description='Python DAG', schedule_interval='*/1 * * * *', start_date=datetime(2020,3,10), catchup=False) as dag:
	ini_task = DummyOperator(task_id='ini_task', retries=3)
	predict_task = PythonOperator(task_id='consume', python_callable=consume)
	end_task = DummyOperator(task_id='end_task')
	ini_task >> predict_task >> end_task