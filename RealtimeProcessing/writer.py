import os
from azure.storage.blob import BlobServiceClient

def write_local(df, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

def write_azure(output_path, azure_conn_str, container_name):
    blob_service_client = BlobServiceClient.from_connection_string(azure_conn_str)
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=os.path.basename(output_path)
    )
    with open(output_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
