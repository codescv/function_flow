#!/bin/bash
source bin/env.sh

# Enable APIs used in the solution
gcloud services enable 'firestore.googleapis.com'
gcloud services enable 'pubsub.googleapis.com'
gcloud services enable 'cloudfunctions.googleapis.com'
gcloud services enable 'appengine.googleapis.com'
gcloud services enable 'cloudscheduler.googleapis.com'
gcloud services enable 'cloudbuild.googleapis.com'
gcloud services enable 'cloudtasks.googleapis.com'
gcloud services enable 'dataflow.googleapis.com'
gcloud app create --region=$GCP_REGION || echo "App already created, skip"
gcloud alpha firestore databases create --region=$GCP_REGION

# Create GCS bucket
gsutil mb gs://$BUCKET_NAME/ || echo "bucket already exists, skip creation"

# Set up pubsub for event notitication
gcloud pubsub topics create $TOPIC_EXTERNAL || echo "topic already exists"

# subscribe BigQuery complete events
gcloud logging sinks create bq_complete_sink pubsub.googleapis.com/projects/$PROJECT_ID/topics/$TOPIC_EXTERNAL \
     --log-filter='resource.type="bigquery_resource" AND protoPayload.methodName="jobservice.jobcompleted"' || echo "sink already exists, skip"
sink_service_account=$(gcloud logging sinks describe bq_complete_sink |grep writerIdentity| sed 's/writerIdentity: //')
echo "bq sink service account: $sink_service_account"
gcloud pubsub topics add-iam-policy-binding $TOPIC_EXTERNAL \
     --member $sink_service_account --role roles/pubsub.publisher

# subscribe Dataflow complete events
gcloud logging sinks create dataflow_complete_sink pubsub.googleapis.com/projects/$PROJECT_ID/topics/$TOPIC_EXTERNAL \
     --log-filter='resource.type="dataflow_step" AND textPayload="Worker pool stopped."' || echo "sink already exists, skip"
sink_service_account=$(gcloud logging sinks describe dataflow_complete_sink |grep writerIdentity| sed 's/writerIdentity: //')
echo "dataflow sink service account: $sink_service_account"
gcloud pubsub topics add-iam-policy-binding $TOPIC_EXTERNAL \
     --member $sink_service_account --role roles/pubsub.publisher

# deploy dataflow pipeline
bash bin/deploy-dataflow-pipeline.sh

# schedule for Poller
# https://cloud.google.com/scheduler/docs/configuring/cron-job-schedules
# For quick cron setting: https://crontab.guru/every-5-minutes
gcloud beta scheduler jobs create pubsub run-poller \
    --schedule '*/5 * * * *' \
    --topic $TOPIC_POLLER \
    --message-body '{}' \
    --time-zone 'America/Los_Angeles' || echo "cron already created, skip"

# Update cloud functions
bash bin/update-functions.sh

echo "Deployment Success"
