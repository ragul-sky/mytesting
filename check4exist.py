import os
from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def test_list_datasets(client):
    """List datasets for a project."""
    # [START bigquery_list_datasets]
    # client = bigquery.Client()
    datasets = list(client.list_datasets())
    project = client.project

    if datasets:
        print('Datasets in project {}:'.format(project))
        for dataset in datasets:  # API request(s)
            print('\t{}'.format(dataset.dataset_id))
    else:
        print('{} project does not contain any datasets.'.format(project))
    # [END bigquery_list_datasets]

def dataset_exists(client, dataset_reference):
    """Return if a dataset exists.
    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        dataset_reference (google.cloud.bigquery.dataset.DatasetReference):
            A reference to the dataset to look for.
    Returns:
        bool: ``True`` if the dataset exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_dataset(dataset_reference)
        return True
    except NotFound:
        return False

client = bigquery.Client(project='pro-area-201010')
test_list_datasets(client)
dataset_ref = client.dataset('avro_data_1')
a = dataset_exists(client,dataset_ref)
print(a)
#a = table_exists(client,'avro_2')
#print (a)
