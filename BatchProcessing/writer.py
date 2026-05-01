import os
import tempfile

from azure.storage.blob import BlobServiceClient

def write_local(df, local_output_path):
    """Writes DataFrame to local parquet file."""
    output_dir = os.path.dirname(local_output_path) or '.'
    os.makedirs(output_dir, exist_ok=True)

    fd, temporary_path = tempfile.mkstemp(suffix='.parquet', dir=output_dir)
    os.close(fd)
    try:
        df.to_parquet(temporary_path, index=False)
        os.replace(temporary_path, local_output_path)
    finally:
        if os.path.exists(temporary_path):
            os.remove(temporary_path)

    print(f"Writer Local: file written to {local_output_path}")

def write_azure(local_output_path, azure_conn_str, container_name):
    """Uploads local file to Azure Blob Storage."""
    if not os.path.exists(local_output_path):
        raise FileNotFoundError(f'Cannot upload missing file: {local_output_path}')

    blob_service_client = BlobServiceClient.from_connection_string(azure_conn_str)
    blob_client = blob_service_client.get_blob_client(
        container=container_name, 
        blob=os.path.basename(local_output_path)
    )
    with open(local_output_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
