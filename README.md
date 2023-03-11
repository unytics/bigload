# bigloader

[![Run on Google Cloud](https://deploy.cloud.run/button.svg)](https://deploy.cloud.run)

## Features

- Config pre-generated as yaml (or UI?)
- state and logs stored at destination
- no docker, no k8s, no database, no instance, no scalability issue

_airbyte_raw_

# Print when writing files to destination!!
# Print_info in utils to be used by destination
# print_airbyte_log function!
# Delete connector!
# Get Logs
# Download should remove venv to!

gcloud builds submit . --tag eu.gcr.io/compte-nickel-datastg/bigloader-source-pypi:latest

gcloud beta run jobs create bigloader-source-pypi --image eu.gcr.io/compte-nickel-datastg/bigloader-source-pypi:latest --region europe-west1

gcloud beta run jobs execute bigloader-source-pypi --region europe-west1