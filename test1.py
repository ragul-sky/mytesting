import os
from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def dataset_exists(client, dataset_reference):
    try:
        client.get_dataset(dataset_reference)
        return True
    except NotFound:
        return False

def table_exists(client, table_reference):
    try:
        client.get_table(table_reference)
        return True
    except NotFound:
        return False

def load_avro_cloud_table(GS_URL):
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'AVRO'
    job_config.write_disposition = 'WRITE_TRUNCATE'
    load_job = client.load_table_from_uri(GS_URL,table_ref, job_config=job_config)
    assert load_job.state == 'RUNNING'
    assert load_job.job_type == 'load'
    load_job.result()  # Waits for table load to complete.
    assert load_job.state == 'DONE'
    print ('Data loaded to table')

key_file_location = r'C:\Projects\avro_test\key\avro-test-e93b546c61fe.json'

credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location)
client = storage.Client(credentials=credentials, project='pro-area-201010')
bucket = client.get_bucket('avro_test_bucket')
blob = bucket.blob('tomatos.avro')
blob.upload_from_filename('tomatos.avro')

print ('Avro data loaded to bucket')

client = bigquery.Client()
dataset_ref = client.dataset('avro_data_2')
check_dataset = dataset_exists(client, dataset_ref)
if check_dataset == False:
    print ('Creating Dataset...')
    dataset_ref = client.dataset('avro_data_2')
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = 'US'
    dataset = client.create_dataset(dataset)

table_ref = dataset_ref.table('avro_1')
check_table = table_exists(client, table_ref)
if check_dataset == False:
    print ('Creating Table...')
    table = bigquery.Table(table_ref)
    table = client.create_table(table)

load_avro_cloud_table('gs://avro_test_bucket/tomatos.avro')


QUERY = (
    'SELECT Year,sum(score) as Sum FROM `avro_data_2.avro_1` '
    'group by Year')
TIMEOUT = 30  # in seconds
dataset = client.dataset('avro_data_2')
new_table_name = 'table_gen'
new_table_ref = dataset.table(new_table_name)
job_config = bigquery.QueryJobConfig()
job_config.destination = new_table_ref
create_instruction = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
write_instruction = bigquery.job.WriteDisposition.WRITE_TRUNCATE
job_config.create_disposition = create_instruction
job_config.write_disposition = write_instruction
query_job = client.query(QUERY,job_config=job_config)  # API request - starts the query
# Waits for the query to finish
iterator = query_job.result(timeout=TIMEOUT)
rows = list(iterator)
assert query_job.state == 'DONE'

#for row in query_job.result():  # Waits for job to complete.
        #print(row)
