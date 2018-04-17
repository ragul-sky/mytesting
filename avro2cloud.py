# Imports the Google Cloud client library
from google.cloud import bigquery

'''# Instantiates a client
bigquery_client = bigquery.Client(project='pro-area-201010')

# The name for the new dataset
dataset_id = 'avro_data_2'

# Prepares a reference to the new dataset
dataset_ref = bigquery_client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)

# Creates the new dataset
dataset = bigquery_client.create_dataset(dataset)'''

#print('Dataset {} created.'.format(dataset.dataset_id)


def load_data_from_file(dataset_id, table_id, source_file_name):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    with open(source_file_name, 'rb') as source_file:
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'AVRO'
        job = bigquery_client.load_table_from_file(
            source_file, table_ref, job_config=job_config)

    job.result()  # Waits for job to complete

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))


client = bigquery.Client()
dataset_ref = client.dataset('avro_data_1')
table_ref = dataset_ref.table('avro_2')
table = bigquery.Table(table_ref)
table = client.create_table(table)

load_data_from_file ("avro_data_1","avro_2","users.avro")
