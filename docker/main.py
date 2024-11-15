import psycopg2
from google.cloud import secretmanager, storage, bigquery
import datetime
import csv
import pandas as pd

def get_db_password(secret_name):
    """Retrieve the database password from Google Secret Manager."""
    secret_m = secretmanager.SecretManagerServiceClient()
    version_name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = secret_m.access_secret_version(request={"name": version_name})
    return response.payload.data.decode("UTF-8")


def connect_to_postgres():
    """Establish a connection to PostgreSQL database."""
    db_params = {
        'dbname': get_db_password('db_postgresql_replica'),
        'user': get_db_password('user_postgresql_replica'),
        'password': get_db_password('pwd_postgresql_replica'),
        'host': get_db_password('host_postgresql_replica'),
        'port': get_db_password('port_postgresql_replica')
    }
    return psycopg2.connect(**db_params)


def fetch_data(table_name):
    """Fetch all data from PostgreSQL without date filtering."""
    connection = connect_to_postgres()
    query = f"SELECT * FROM {table_name} "
    try:
        with connection.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=col_names)
            q2 = "SELECT column_name, data_type  FROM information_schema.columns WHERE table_name = '" + table_name + "';"
            cur.execute(q2)
            col_type = cur.fetchall()
            re = pd.DataFrame(col_type, columns=['column_name', 'data_type'])
            # change writing mode from df to dict for ex
            return df, re
    except Exception as e:
        print(f"Error fetching data: {e}")
        return [], []
    finally:
        connection.close()


def enforce_column_types(df, schema_df):
    type_mapping = {
        'INTEGER': 'int64',
        'FLOAT64': 'float64',
        'STRING': 'object',
        'TIMESTAMP': 'datetime64[ns]',
        'BOOLEAN': 'bool'
    }

    for _, row in schema_df.iterrows():
        column_name = row['column_name']
        expected_type = row['data_type']

        if column_name in df.columns:
            try:
                df[column_name] = df[column_name].astype(type_mapping[expected_type])
            except Exception as e:
                e = 'error'
    return df

def upload_to_gcs(bucket_name, destination_blob_name, data, col_names):
    """Uploads data to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    with open('dict.csv', 'w') as csv_file:
        writer = csv.writer(csv_file)
        for key, value in data.items():
            writer.writerow([key, value])

    # Upload to GCS
    # blob.upload_from_file(output, content_type='text/csv')
    print(f"File uploaded to {destination_blob_name}.")


def load_gcs_to_bigquery(dataset_id, table_id, source_uri):
    """Loads a CSV file from Google Cloud Storage to BigQuery."""
    bigquery_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        null_marker='',
        skip_leading_rows=1,  # Ignore la première ligne si elle contient des en-têtes
        write_disposition="WRITE_APPEND",  # Ajoute les données à la table existante
        field_delimiter=',',  # Définir le séparateur des colonnes (par défaut : ',')
        encoding='UTF-8'  # Définir l'encodage (par défaut : 'UTF-8')
    )
    print(source_uri)
    load_job = bigquery_client.load_table_from_uri(
        source_uri,
        f"{dataset_id}.{table_id}",
        job_config=job_config,
    )

    print(f"Starting job {load_job.job_id}")
    load_job.result()  # Wait for the job to complete

    destination_table = bigquery_client.get_table(f"{dataset_id}.{table_id}")
    print(f"Loaded {destination_table.num_rows} rows into {dataset_id}:{table_id}.")





def generate_bq_schema(df):
    df.data_type = df.data_type.replace({'character varying': 'STRING',
                                         'integer': 'INTEGER',
                                         'timestamp without time zone': 'TIMESTAMP',
                                         'bytea': 'STRING',
                                         'text': 'STRING',
                                         'boolean': 'BOOLEAN',
                                         'character': 'STRING',
                                         'numeric': 'FLOAT64',
                                         'double precision': 'FLOAT64'})
    schema = []
    for _, row in df.iterrows():
        schema.append(bigquery.SchemaField(row['column_name'], row['data_type']))
    return schema


def load_df_to_bq(bq, df, table_location, schema_df):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        encoding='UTF-8',
        schema=generate_bq_schema(schema_df),
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingestion_date",  # Name of the column to use for partitioning.
            expiration_ms=7776000000,  # 90 days.
        )
    )
    df = enforce_column_types(df, schema_df)
    df['ingestion_date'] = datetime.datetime.now()
    job = bq.load_table_from_dataframe(
        df, table_location, job_config=job_config
    )
    return job


def main():
    project_id = '' #into variable
    dataset_id = ''  #into variable
    bq = bigquery.Client(project=project_id)
    query_job = bq.query(f'select table_name from {project_id}.platform.tables_list')
    table_list = query_job.result().to_dataframe().table_name.to_list()
    for table_name in table_list:
        table_location = f'{project_id}.{dataset_id}.{table_name}_history'
        # Fetch data from PostgreSQL
        df, schema_df = fetch_data(table_name)
        print(f"Fetched {table_name} at : ", datetime.datetime.now())

        if df.shape[0] > 0:
            load_df_to_bq(bq, df, table_location, schema_df)
            print(f"{table_name} uploaded at : ", datetime.datetime.now())
        else:
            print(f"No data fetched for dataset {table_name}")
        del df


if __name__ == "__main__":
    print("Started at : ", datetime.datetime.now())
    main()
    print("Ended at : ", datetime.datetime.now())
