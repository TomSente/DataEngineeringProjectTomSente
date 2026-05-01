import os
from azure.storage.blob import BlobServiceClient

def write_local(df, local_output_path):
    """Writes DataFrame to local parquet file."""
    os.makedirs(os.path.dirname(local_output_path), exist_ok=True)
    df.to_parquet(local_output_path, index=False)
    print(f"Writer Local: file written to {local_output_path}")

def write_azure(local_output_path, azure_conn_str, container_name):
    """Uploads local file to Azure Blob Storage."""
    blob_service_client = BlobServiceClient.from_connection_string(azure_conn_str)
    blob_client = blob_service_client.get_blob_client(
        container=container_name, 
        blob=os.path.basename(local_output_path)
    )
    with open(local_output_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
