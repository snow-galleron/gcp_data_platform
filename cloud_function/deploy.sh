gcloud functions deploy VM \
    --runtime python310 \
    --trigger-http \
    --allow-unauthenticated \
    --entry-point start_instance \
    --region europe-west1 \
    --service-account {{service_account_email}} \
    --gen2
