#!/bin/bash
if [[ -z $PROJECT_ID ]]; then
  PROJECT_ID=$GOOGLE_CLOUD_PROJECT
fi

if [[ -z $BUCKET_NAME ]]; then
  BUCKET_NAME=$PROJECT_ID
fi

# GCP region
if [[ -z $GCP_REGION ]]; then
  GCP_REGION='asia-northeast1'
fi

TOPIC_POLLER='POLLER_CRON'
TOPIC_SCHEDULE='SCHEDULE'
TOPIC_EXTERNAL='SCHEDULE_EXTERNAL_EVENTS'

# source code directory
SRC_DIR='src'
PYTHON='python3'
PIP='pip3'

set -euxo pipefail
gcloud config set project $PROJECT_ID
echo "Project id: $PROJECT_ID  bucket: $BUCKET_NAME region: $GCP_REGION"
