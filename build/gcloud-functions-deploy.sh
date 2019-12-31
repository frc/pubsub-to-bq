#!/bin/sh

gcloud functions deploy subInsertBq \
    --region="europe-west1" --runtime="nodejs10" \
    --timeout="10s" --retry \
    --no-allow-unauthenticated \
    --trigger-topic="insert-to-bq"
