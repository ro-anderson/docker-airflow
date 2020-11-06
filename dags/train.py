from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

from datetime import datetime
import urllib.request
import json


def get_data():
	url = 'http://flask-nginx/prediction/api/v1.0/getdata'
	data = {'params': ['param_0', 'param_1', 'param_n']}
	data = json.dumps(data)
	data = data.encode('utf-8')

	req = urllib.request.Request(url)
	req.add_header('Content-Type', 'application/json; charset=utf-8')
	req.add_header('Content-Length', len(data))

	response = urllib.request.urlopen(req, data)
	response = response.read().decode('utf-8')
	response = json.loads(response)
	print(response)
	

def proc_data():
	url = 'http://flask-nginx/prediction/api/v1.0/procdata'
	data = {'params': ['param_0', 'param_1', 'param_n']}
	data = json.dumps(data)
	data = data.encode('utf-8')

	req = urllib.request.Request(url)
	req.add_header('Content-Type', 'application/json; charset=utf-8')
	req.add_header('Content-Length', len(data))

	response = urllib.request.urlopen(req, data)
	response = response.read().decode('utf-8')
	response = json.loads(response)
	print(response)

def train_model():
	url = 'http://flask-nginx/prediction/api/v1.0/train'
	data = {'params': ['param_0', 'param_1', 'param_n']}
	data = json.dumps(data)
	data = data.encode('utf-8')

	req = urllib.request.Request(url)
	req.add_header('Content-Type', 'application/json; charset=utf-8')
	req.add_header('Content-Length', len(data))

	response = urllib.request.urlopen(req, data)
	response = response.read().decode('utf-8')
	response = json.loads(response)
	model_path = response['model_path']
	print(response)
	return response


def validate_model(**kwargs):
	url = 'http://flask-nginx/prediction/api/v1.0/validate'
	req = urllib.request.Request(url)
	response = urllib.request.urlopen(req)
	response = response.read().decode('utf-8')
	response = json.loads(response)
	print(response)
	valid = response['status'] == 'ok'

	if valid:
		return 'train_new_model'
	else:
		return 'restore_previous_model'


def restore_model():
	print('Restoring previous model')
	url = 'http://flask-nginx/prediction/api/v1.0/restore'


def set_new_model(**context):
	print('Setting new model')
	url = 'http://flask-nginx/prediction/api/v1.0/setnewmodel'

	model_path = context['task_instance'].xcom_pull(task_ids='train_new_model')['model_path']
	data = {'model_path': model_path}
	data = json.dumps(data)
	data = data.encode('utf-8')

	req = urllib.request.Request(url)
	req.add_header('Content-Type', 'application/json; charset=utf-8')
	req.add_header('Content-Length', len(data))

	response = urllib.request.urlopen(req, data)
	response = response.read().decode('utf-8')
	response = json.loads(response)
	print(response)


 
with DAG('train_api', description='Python DAG', schedule_interval='*/1 * * * *', start_date=datetime(2020,3,10), catchup=False) as dag:
	ini_task = DummyOperator(task_id='ini_task', retries=3)
	get_data_task = PythonOperator(task_id='get_data', python_callable=get_data)
	proc_data_task = PythonOperator(task_id='proc_data', python_callable=proc_data)
	train_model_task = PythonOperator(task_id='train_new_model', python_callable=train_model)
	restore_model_task = PythonOperator(task_id='restore_previous_model', python_callable=train_model)
	set_new_model_task = PythonOperator(task_id='set_new_model', python_callable=set_new_model, provide_context=True)
	end_task = DummyOperator(task_id='end_task', trigger_rule='none_failed')
	branch = BranchPythonOperator(task_id='validate_new_model', python_callable=validate_model, provide_context=True)

	ini_task >> get_data_task >> proc_data_task >> branch >> [train_model_task, restore_model_task]
	train_model_task >> set_new_model_task >> end_task
	restore_model_task >> end_task
