from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import os
from google.cloud import bigquery


credentials_dict = {
    'type': 'service_account',
    'client_id': '117391907748603090984',
    'client_email': "ragul-493@pro-area-201010.iam.gserviceaccount.com",
    'private_key_id': 'e93b546c61fef556e18498658f7259a5fcc3896d',
    "private_key":  "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCu+fSpqltCkySa\n6E3SBQSigz+WVEqu7OAnuo3lOlUCvktk7GDaGQsO+KNIJg8HawnZ8lvUvY7o5fEe\nKbWzvOuTuBFEnhQ81beFG4e+2ytHNBNBaNUotqfeZZRAb2URz34EMZ+z6Rgc4oXa\nlQVLapmOJfBuSlMiOQ6aKUacMzsEHejHJwa/aDImgTtrVEAHYEiDJke5JLHPTwli\nt/tZQCAvQFNX6KYXGNumZ43SFB74c6OOQwDG5iBzsix7jmGgRcBW8G10xWxy0XQm\nhMhFY2FfT/ZLbUUogsJEftW0I4OGxLOoSxJhTYebwK3RTvwWZpRpfleB8vXJhiBU\nP7MwKowfAgMBAAECggEAG+k3Q9Eu2YHLNwKTV0sBoEK7Y+lyOFEEuJGjjMsvQE7G\njATAtkmEYBD9Ssb3YsGKQr/MMjVClJgJTDxvBYq/MUMNThrBM6sLqSnpD2K6lpMR\n9z8XsXThdjJR4a1jO+ITQewAg5xNAl6H54QZ4hvQ/sHChnVIqY7ca27mGoh5g4BV\nr6DrUiPTJ1GOof4htN2gEFFLfLteIdLlVoH77i+0QX57CnMefnvQ2m3y5nL0F4Pv\nhC7MRIG5lJ44m3w07GKZjbHAHOSZzfFxkmDH1J1p1oHAIkQxUvMk+EXGzzlx6mCu\nEJw6VviXY+7+IT5Cmrm0/Y6nFBWtJTmfsc++/RgPcQKBgQDzgZ8s/sxTsXURsHTo\n5s/aVPav9BH6hmc32kaxaL6loUHR7bu0watmKGGT67ukErTU7npYebn5ZqHndRFd\nSO4xzuXb99ER84c62UVYbYXI5rfoL7P+XdiRx2wAF0eBHl1+++8OmD9QBjlFSnpL\nVoMlgMSkvVYfj1hXwKvh35PyowKBgQC39DcEI9sp83A5Hp5nqCW8pGG/4YHrnIg8\nqLwQrosdw45vLDdmi0tKJGBzvlXe/m3GWB+g7EwVQ6DRcrTXEoqwkBco9OFh7ab/\nRN075NzmGckdJMbBTYS5cOi8tUrdPTD6XBRv2Nolcy/ixCPKiTkZnC8UT6NrJIMt\nCoGHojTUVQKBgFdz+SfexqFUPVhCCGrQ4ltmYkZlIMPZ214qzXLPzJCNrqbne596\nYsVIwWqvoe9WLI3ArD30Ub9phcIrwDXBiJJo3RK4GOUakjmnaIlOpFq3ydrgrwMu\nzyXj+jy6H5sWvbtqXbvfH/Xslkr1aaB/DJC+g2ErArBI+hBiCRkRBACDAoGBAJps\n+NaP50ejZr1818v7G5rugGTdwfxyTuhlWmPCs2TMK0Fm5CdAnnbibNodmeb2vl74\n0ax9Ui2ztUi/O9MlbKpmfBrU4HQeVsgLHpJOQaclVHET04qpMdefUrJAm1V5Limu\nrTVr3U39EeETeVxosDewptEtPeoPH9Pq8uxTHnh1AoGAAVh3VKYNQ3vVW83otUoC\n+fERf6zsa/ZofXUMKftDwXbluleoO0I1DBCPK+jLXFNOH+5rs47oq5znosrXdi0b\n7e4RbhSsvw8dm0VcM1yvJUNSJ+vcdrel+yKuCN9FhrF1x9/w81eGkHYLpkfJ7C68\nrIV1Cij1DCCzeRARDD+kqdM=\n-----END PRIVATE KEY-----\n"
}
credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    credentials_dict
)
client = storage.Client(credentials=credentials, project='pro-area-201010')
bucket = client.get_bucket('avro_test_bucket')
blob = bucket.blob('users.avro')
blob.upload_from_filename('users.avro')


client = bigquery.Client()
dataset_ref = client.dataset('avro_data_1')
table_ref = dataset_ref.table('avro_3')
table = bigquery.Table(table_ref)
table = client.create_table(table)

job_config = bigquery.LoadJobConfig()
GS_URL = 'gs://avro_test_bucket/users.avro'
job_config.source_format = 'AVRO'
load_job = client.load_table_from_uri(GS_URL,table_ref, job_config=job_config)

assert load_job.state == 'RUNNING'
assert load_job.job_type == 'load'

load_job.result()  # Waits for table load to complete.

assert load_job.state == 'DONE'
