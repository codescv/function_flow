# coding=utf-8
# Copyright 2021 Google LLC..
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

import datetime
import uuid
import json
import logging
import traceback
from typing import Any, Mapping, Optional

from function_flow import futures
from function_flow.futures import _get_value
from function_flow import tasks

import googleapiclient.discovery
import google.auth
import google.api_core.exceptions
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import tasks_v2
from google.cloud.tasks_v2.services import cloud_tasks
from google.cloud.tasks_v2 import types
from google.protobuf import timestamp_pb2

# work around GCP location inconsistancy
# https://cloud.google.com/appengine/docs/locations
_LOCATION_MAP = {'us-central': 'us-central1', 'europe-west': 'europe-west1'}


class CloudTaskFuture(futures.Future):
  CLOUD_TASK_EVENT_TYPE = 'cloud_task_event'

  @classmethod
  def handle_message(cls, message: Mapping[str,
                                           Any]) -> Optional[futures.Result]:
    """Handles Cloud tasks messages.

    Args:
      message: The message JSON dictionary.

    Returns:
      Parsed task result from the message or None.
    """
    if _get_value(message,
                  'function_flow_event_type') == cls.CLOUD_TASK_EVENT_TYPE:
      queue_id = _get_value(message, 'queue_id')
      status = _get_value(message, 'status')
      if status == 'SUCCESS':
        return futures.Result(trigger_id=queue_id, is_success=True)
      else:
        error = _get_value(message, 'error')
        return futures.Result(
            trigger_id=queue_id, is_success=False, error=error)
    else:
      return None


class CloudTaskHelper:
  """Ref https://cloud.google.com/tasks/docs/creating-http-target-tasks"""
  CLOUD_TASK_COLLECTION = 'CloudTaskStatus'

  def __init__(
      self,
      project=None,
      location=None,
      event_topic='SCHEDULE_EXTERNAL_EVENTS',
      queue_id_prefix='queue',
      db=None,
      pubsub=None,
      cloud_tasks_client=None,
  ):
    # TODO: get cloud task location automatically
    self.db = db or firestore.Client()
    self.pubsub = pubsub or pubsub_v1.PublisherClient()
    self.cloud_tasks = cloud_tasks_client or cloud_tasks.CloudTasksClient()

    if not project:
      _, project = google.auth.default()
    self.project = project
    self.location = location
    if not self.location:
      gae = googleapiclient.discovery.build('appengine', 'v1')
      result = gae.apps().get(appsId=self.project).execute()
      self.location = _LOCATION_MAP.get(result['locationId'],
                                        result['locationId'])
    logging.info('Cloud task location: %s', self.location)

    self.queue_id_prefix = queue_id_prefix
    self.queue_id = None
    self.topic_path = self.pubsub.topic_path(self.project, event_topic)

  def future(self):
    if not self.queue_id:
      raise Exception('must call start before calling future!')
    return CloudTaskFuture(trigger_id=self.queue_id)

  def start(self,
            worker_func_name='pingback_worker',
            worker_func_region='us-central1',
            num_workers=1,
            params_func=None,
            max_dispatches_per_second=1,
            max_burst_size=1,
            max_concurrent_dispatches=1):
    parent = f'projects/{self.project}/locations/{self.location}'
    rand_id = uuid.uuid4().hex[:4]
    now = datetime.datetime.utcnow()
    time_str = now.strftime('%Y-%m-%d-%H%M')
    self.queue_id = f'{self.queue_id_prefix}-{time_str}-{rand_id}'
    queue_path = self.cloud_tasks.queue_path(self.project, self.location,
                                             self.queue_id)

    self.cloud_tasks.create_queue(
        parent=parent,
        queue=types.Queue(
            name=queue_path,
            rate_limits={
                'max_dispatches_per_second': max_dispatches_per_second,
                'max_burst_size': max_burst_size,
                'max_concurrent_dispatches': max_concurrent_dispatches
            }))

    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(now)

    queue_ref = self.db.document(self.CLOUD_TASK_COLLECTION, self.queue_id)
    queue_ref.set({
        'start_time': time_str,
        'num_workers': num_workers,
        'finished': 0,
        'status': 'RUNNING'
    })

    for task_id in range(num_workers):
      task_name = f'task-{task_id}'
      params = {}
      if params_func:
        params = params_func(task_id)

      task_ref = queue_ref.collection('workers').document(task_name)
      task_ref.set({
          'start_time': now.strftime('%Y-%m-%d-%H:%M'),
          'status': 'READY',
          'params': params
      })

      payload = {'queue_id': self.queue_id, 'worker_id': task_id}

      task = {
          'name':
              self.cloud_tasks.task_path(self.project, self.location,
                                         self.queue_id, task_name),
          'http_request': {
              'http_method':
                  tasks_v2.HttpMethod.POST,
              # TODO: how to get this url automatically?
              'url':
                  f'https://{worker_func_region}-{self.project}.cloudfunctions.net/{worker_func_name}',
              'headers': {
                  'Content-type': 'application/json'
              },
              'body':
                  json.dumps(payload).encode('utf-8'),
              # TODO: how to set up authentication?
              'oidc_token': {
                  'service_account_email':
                      f'{self.project}@appspot.gserviceaccount.com'
              },
          },
          'schedule_time':
              timestamp
      }

      response = self.cloud_tasks.create_task(request={
          'parent': queue_path,
          'task': task
      })
      logging.info('Created task response: %s', response)

  def delete_queue(self, queue_id):
    # delete cloud task queue
    # https://googleapis.dev/python/cloudtasks/latest/tasks_v2/services.html
    try:
      queue_name = f'projects/{self.project}/locations/{self.location}/queues/{queue_id}'
      self.cloud_tasks.delete_queue(name=queue_name)
      logging.info('deleted queue from db: %s', queue_name)
    except:
      logging.exception('unable to delete queue %s', queue_name)

  def wraps(self, worker_func):
    # TODO: cleanup queue if finished
    def _worker_func(request: 'flask.Request'):
      request_json = request.get_json()
      queue_id = request_json['queue_id']

      queue_ref = self.db.document(self.CLOUD_TASK_COLLECTION, queue_id)
      task_id = request_json['worker_id']
      task_name = f'task-{task_id}'
      task_ref = queue_ref.collection('workers').document(task_name)
      task_ref.update({'status': 'RUNNING'})

      error = None
      try:
        params = task_ref.get().to_dict()['params']
        result = worker_func(task_id, params)
        task_ref.update({
            'status':
                'SUCCESS',
            'finish_time':
                datetime.datetime.utcnow().strftime('%Y-%m-%d-%H:%M'),
            'result':
                result
        })
        queue_ref.update({'finished': firestore.Increment(1)})
      except:
        logging.exception('error running task in queue: %s', queue_id)
        error = traceback.format_exc()

      if error:
        queue_ref.update({'status': 'FAILED', 'failed_task': task_name})

        task_ref.update({'status': 'ERROR', 'error': error})

        message = {
            'function_flow_event_type': CloudTaskFuture.CLOUD_TASK_EVENT_TYPE,
            'status': 'FAILED',
            'error': error,
            'queue_id': queue_id
        }
        data = json.dumps(message).encode('utf-8')
        self.pubsub.publish(self.topic_path, data=data)

        self.delete_queue(queue_id)
      else:
        # check queue completion
        queue_ref = self.db.document(self.CLOUD_TASK_COLLECTION, queue_id)
        queue_dict = queue_ref.get().to_dict()
        logging.info('queue status: %s', queue_dict)
        if queue_dict['num_workers'] == queue_dict['finished']:
          # The queue is finished
          queue_ref.update({
              'status':
                  'SUCCESS',
              'finish_time':
                  datetime.datetime.utcnow().strftime('%Y-%m-%d-%H:%M')
          })
          logging.info('queue finished: %s', queue_id)
          message = {
              'function_flow_event_type': CloudTaskFuture.CLOUD_TASK_EVENT_TYPE,
              'status': 'SUCCESS',
              'queue_id': queue_id
          }
          data = json.dumps(message).encode('utf-8')
          self.pubsub.publish(self.topic_path, data=data)
          self.delete_queue(queue_id)

    return _worker_func
