#!/bin/bash

source bin/env.sh

gcloud functions deploy start --runtime python37 --trigger-http --source $SRC_DIR --quiet
gcloud functions deploy scheduler --runtime python37 --trigger-topic $TOPIC_SCHEDULE --source $SRC_DIR --quiet
gcloud functions deploy external_event_listener --runtime python37 --trigger-topic $TOPIC_EXTERNAL --source $SRC_DIR --quiet
# function for polling
gcloud functions deploy poller --timeout=540 --runtime python37 --trigger-topic $TOPIC_POLLER --source src --quiet