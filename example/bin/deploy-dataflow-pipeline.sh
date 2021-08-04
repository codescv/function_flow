#!/bin/bash

source bin/env.sh

pip install -U 'apache-beam[gcp]'
DATAFLOW_PREFIX="gs://$PROJECT_ID/dataflow"
python -m src.dataflow_main \
    --runner DataflowRunner \
    --project $PROJECT_ID \
    --staging_location $DATAFLOW_PREFIX/staging \
    --temp_location $DATAFLOW_PREFIX/temp \
    --template_location $DATAFLOW_PREFIX/templates/function_flow_example \
    --region $GCP_REGION \
    --noauth_local_webserver