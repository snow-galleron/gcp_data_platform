import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2
from google.cloud import secretmanager
import datetime
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from google.auth import default

try:
    _, project_id = default()
    print(f"Project ID: {project_id}")
except Exception as e:
    print(f"Erreur lors de la récupération du Project ID: {e}")


def get_secret(secret_id):
    """Retrieves a secret from Secret Manager."""
    secret_m = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest" #Use latest version
    try:
        response = secret_m.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error retrieving secret {secret_id}: {e}")
        return None


def run_pipe():
    print("Created at : ", datetime.datetime.now())

    # Retrieve all parameters from Secret Manager
    dataset_id = get_secret("dataset_id")
    table_list = get_secret("table_list").split(',') # Assumes a comma-separated string
    db_user = get_secret("db_user")
    db_password = get_secret("db_password")
    db_name = get_secret("db_name")
    db_host = get_secret("db_host")
    db_port = get_secret("db_port") # Port is now a string, will be converted to int if needed.


    if not all([project_id, dataset_id, table_list, db_user, db_password, db_name, db_host, db_port]):
      print("Error: Failed to retrieve one or more secrets from Secret Manager.")
      return

    # Handle potential errors during type conversion
    try:
        db_port = int(db_port)
    except ValueError:
        print("Error: Invalid db_port value in Secret Manager.  Must be an integer.")
        return

    options = PipelineOptions(
        project=project_id,
        runner='DataflowRunner',
        region='europe-west1',  # Still needs a region
        job_name='v3',
        staging_location=f'gs://{get_secret("staging_bucket")}/staging', # Get staging bucket from secret manager
        temp_location=f'gs://{get_secret("temp_bucket")}/temp' # Get temp bucket from secret manager
    )

    jdbc_url = f'jdbc:postgresql://{db_host}:{db_port}/{db_name}'

    with beam.Pipeline(options=options) as p:
        for table_name in table_list:
            # Dynamically determine columns
            cols = []
            try:
                conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
                cur = conn.cursor()
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                cols = [item[0] for item in cur.fetchall()]
                cur.close()
                conn.close()
            except psycopg2.Error as e:
                print(f"Error connecting to PostgreSQL or retrieving columns for {table_name}: {e}")
                continue #Skip to next table if error occurs

            (p
             | f'ReadFromPostgres_{table_name}' >> ReadFromJdbc(
                    driver_class_name='org.postgresql.Driver',
                    jdbc_url=jdbc_url,
                    username=db_user,
                    password=db_password,
                    table_name=table_name,
                    query=f'SELECT * FROM {table_name}'
                )
             | f'Format for BigQuery_{table_name}' >> beam.Map(lambda row: {col: value for col, value in zip(cols, row)}) # Simplified formatting
             | f'Write to BigQuery_{table_name}' >> beam.io.WriteToBigQuery(
                        table=f'{project_id}:{dataset_id}.{table_name}',
                        schema='SCHEMA_AUTODETECT',
                        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                    ))
    print("Ended at : ", datetime.datetime.now())


run_pipe()
