# coding=utf-8
# Copyright 2022 Google LLC..
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example for function flow."""

import functools
import json
import logging
import os
import uuid

from googleapiclient.discovery import build
from function_flow import futures
from function_flow import tasks

import google.api_core.exceptions
import google.auth
from google.cloud import bigquery
from google.cloud import storage

example_job = tasks.Job(
    name='test_job', schedule_topic='SCHEDULE', max_parallel_tasks=3)

# The cloud function to schedule next tasks to run.
scheduler = example_job.make_scheduler()

# The cloud function triggered by external events(e.g. finished bigquery jobs)
external_event_listener = example_job.make_external_event_listener()

# poller
poller = example_job.make_poller()

# The task DAG looks like this:
# gen_dataset      gen_data
#         \          /    \
#       load_data_to_bq   extract_content
#              |                 |
#         query_table      view_content
#              |
#      get_query_results

# extract_content is an async DataFlow task.
# load_data_to_bq is an async task triggered by log router via PubSub.
# query_table is an async BigQuery task.


@example_job.task(task_id='gen_dataset')
def gen_dataset(task: tasks.Task, job: tasks.Job) -> str:
  """Creates a dataset in bq."""
  del task  # unused

  client = bigquery.Client()
  dataset_name = job.get_arguments().get('dataset') or 'ff_test_dataset'
  dataset_id = f'{client.project}.{dataset_name}'
  dataset = bigquery.Dataset(dataset_id)
  try:
    dataset = client.create_dataset(dataset)  # Make an API request.
    logging.info('Created dataset %s', dataset_id)
  except google.api_core.exceptions.Conflict:
    # ignore if already exists
    logging.info('dataset already exists %s', dataset_id)

  return dataset_id


@example_job.task(task_id='gen_data')
def gen_data(task: tasks.Task, job: tasks.Job) -> str:
  """Writes some fake data into gcs."""

  del task  # unused

  fake_data = [
      {
          'id': '1',
          'content': 'foo',
          'value': 3
      },
      {
          'id': '2',
          'content': 'bar',
          'value': 6
      },
      {
          'id': '3',
          'content': 'foo',
          'value': 2
      },
      {
          'id': '4',
          'content': 'fba',
          'value': 7
      },
  ]

  job_args = job.get_arguments()
  _, project_id = google.auth.default()
  bucket_name = job_args.get('bucket') or project_id
  source_file_name = os.path.join('/tmp/file.json')

  with open(source_file_name, 'w') as f:
    for data in fake_data:
      f.write(json.dumps(data))
      f.write('\n')

  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  destination_blob_name = 'data/file.json'
  blob = bucket.blob(destination_blob_name)
  blob.upload_from_filename(source_file_name)

  uri = f'gs://{bucket_name}/{destination_blob_name}'
  os.remove(source_file_name)

  return uri


@example_job.task('extract_content', deps=['gen_data'])
def extract_content(task, job):
  """Dataflow job to extract content field from the output of gen_data."""
  del task  # unused

  _, project_id = google.auth.default()

  input_uri = job.get_task_result('gen_data')

  parameters = {
      'input_path': input_uri,
      'output_path': f'gs://{project_id}/data/output.json',
  }

  job = 'ff-example-job-' + uuid.uuid1().hex[:8]

  dataflow = build('dataflow', 'v1b3')
  # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.templates/launch
  # machine types:
  # https://cloud.google.com/compute/docs/machine-types#general_purpose
  request = dataflow.projects().templates().launch(
      projectId=project_id,
      gcsPath=f'gs://{project_id}/dataflow/templates/function_flow_example',
      body={
          'jobName': job,
          'parameters': parameters,
          'environment': {
              'numWorkers': 1,
              'maxWorkers': 1,
              'machineType': 'n1-standard-2',
          }
      })
  response = request.execute()
  logging.info('dataflow job launch response: %s', response)

  job_id = response['job']['id']
  return futures.DataFlowFuture(job_id)


@example_job.task('view_content', deps=['extract_content'])
def view_content(task, job):
  """Check output of Dataflow job."""

  del task, job  # unused

  _, project_id = google.auth.default()

  client = storage.Client()
  bucket = client.get_bucket(project_id)

  results = []
  for blob in bucket.list_blobs(prefix='data/output.json', max_results=5):
    results.append(blob.name)

  return json.dumps({'results': results})


@example_job.task('load_data_to_bq', deps=['gen_dataset', 'gen_data'])
def load_data_into_bq(task: tasks.Task, job: tasks.Job) -> str:
  """Loads data into BigQuery table asynchronously."""
  del task  # unused

  job_args = job.get_arguments()
  dataset_name = job_args.get('dataset') or 'ff_test_dataset'
  table_name = job_args.get('table') or 'ff_test_table'
  client = bigquery.Client()
  dataset_id = f'{client.project}.{dataset_name}'

  data_uri = job.get_task_result('gen_data')

  # create table if not exists
  table_id = f'{dataset_id}.{table_name}'
  schema = [
      bigquery.SchemaField('id', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('content', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('value', 'INTEGER', mode='REQUIRED'),
  ]
  table = bigquery.Table(table_id, schema=schema)
  try:
    table = client.create_table(table)  # Make an API request.
    logging.info('Created table %s', table_id)
  except google.api_core.exceptions.Conflict:
    # ignore if already exists
    logging.info('Table already exists %s', table_id)

  table_ref = client.dataset(dataset_name).table(table_name)

  job_config = bigquery.LoadJobConfig()
  job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
  job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

  load_job = client.load_table_from_uri(
      data_uri, table_ref, job_config=job_config)

  return futures.BigQueryFuture(load_job.job_id)


@example_job.task('query_table', deps=['load_data_to_bq'])
def query_table(task: tasks.Task, job: tasks.Job):
  """Queries the BQ Table asynchronously."""
  del task  # unused

  job_args = job.get_arguments()
  dataset_name = job_args.get('dataset') or 'ff_test_dataset'
  table_name = job_args.get('table') or 'ff_test_table'

  client = bigquery.Client()

  table = f'{client.project}.{dataset_name}.{table_name}'
  sql = f"""
SELECT count(*) as count, sum(value) as value, content
FROM `{table}`
GROUP BY content
  """

  logging.info('querying: %s', sql)
  bq_job = client.query(sql)

  return futures.BigQueryFuture(bq_job.job_id)


@example_job.task('get_query_results', deps=['query_table'])
def get_query_results(task: tasks.Task, job: tasks.Job) -> str:
  """Fetch the query results."""
  del task  # unused
  q_result = job.get_task_result('query_table')
  logging.info('query result: %s', q_result)
  client = bigquery.Client()
  bq_job = client.get_job(q_result['job_id'], location=q_result['location'])

  results = {}
  for row in bq_job.result():
    results[row['content']] = {'count': row['count'], 'value': row['value']}

  return json.dumps(results)


def log_errors(func):
  """Wraps and print exceptions to work around a cloud funtion bug.

    Ref: https://issuetracker.google.com/155215191

  Args:
    func: the wrapped function

  Returns:
    Wrapped function.
  """

  @functools.wraps(func)
  def wrapper(*args, **kwargs):
    try:
      result = func(*args, **kwargs)
      return result
    except:
      logging.exception('Error encountered in %s', func)

  return wrapper


@log_errors
def start(request: 'flask.Request') -> str:
  """The workflow entry point."""
  request_json = request.get_json()
  example_job.start(request_json)
  return json.dumps({'id': example_job.id})
