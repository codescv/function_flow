# Function Flow

Function Flow is a workflow management framework on top of Google Cloud
Functions.

It allows you to write `tasks` in Python functions, define their dependencies
and manage the executions for you.

For example, if you want to first transform data from BigQuery, then invoke a
job to AI platform to make some predictions, after that load some data from GCS
to BigQuery, this can be a helpful tool for you.

## Running the Example

To enable all services, and deploy all cloud functions, first create a blank GCP
project, then run the following command:

```bash
cd example
bash bin/deploy.sh
```

If you edited `example/src/main.py`, you can run `bash bin/update-fuctions.sh`
for the code to take effect.

To run the example, go to cloud function page and run the function `start`. Then
you can view the task statuses in the Firestore UI.
