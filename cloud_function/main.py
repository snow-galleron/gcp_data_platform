from google.cloud import compute_v1, secretmanager_v1
from google.auth import default
import time
import subprocess
import time
import os

def start_instance(request):
    project_id = ''
    zone = 'europe-west1-b'
    instance_name = 'VM'
    secret_name = 'https_token_github'
    github_username = 'user_name'
    script_to_run = ''

    credentials, _ = default()
    instance_client = compute_v1.InstancesClient(credentials=credentials)
    secret_client = secretmanager_v1.SecretManagerServiceClient(credentials=credentials)

    # Récupérer le jeton GitHub depuis Secret Manager
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    secret_response = secret_client.access_secret_version(request={"name": secret_path})
    github_token = secret_response.payload.data.decode('UTF-8')

    # Démarrer la VM
    operation = instance_client.start(
        project=project_id,
        zone=zone,
        instance=instance_name
    )


    # Attendre que l'instance soit en état "RUNNING"
    while True:
        instance = instance_client.get(project=project_id, zone=zone, instance=instance_name)
        if instance.status == 'RUNNING':
            break
        time.sleep(5)


    command = (
        'sudo apt-get update && '
        'sudo apt-get install -y docker.io && '
        'sudo gcloud auth configure-docker && '
        'sudo usermod -aG docker $USER && '
        f'container_id=$(sudo docker run -d gcr.io/{project_id}/odoocker) && '
        'sudo docker wait $container_id && '
        'sudo shutdown -h now'
    )
    ssh_command = f"gcloud compute ssh {instance_name} --zone={zone} --command \"{command}\""
    os.system(ssh_command)

    return f'Started instance {instance_name}, cloned the repo, and ran {script_to_run}.'

start_instance(1)
