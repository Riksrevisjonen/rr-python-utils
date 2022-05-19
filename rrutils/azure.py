from azure.storage.blob import BlobClient, ContainerClient
import logging

def get_blob_names(
    container_name: str, 
    account_url: str = None, 
    credential: object = None, 
    conn_str: str = None): 
    """Get the names of all blobs in an Azure blob container"""
    if conn_str is None: 
        if account_url is None or credential is None:
            logging.error('Both account url and credential must be provided!')
            return
        if not isinstance(credential, DefaultAzureCredential):
            logging.error('You must use a valid Azure credential')
        container = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=credential)
    else: 
        if conn_str is None:
            logging.error('Connection string cannot be empty!')
            return
        container = ContainerClient.from_connection_string(
            conn_str=conn_str, 
            container_name=container_name)
    blob_list = container.list_blobs()
    blob_names = []
    for blob in blob_list:
        blob_names.append(blob.name)
    return blob_names

def get_blob(
    container_name: str,
    blob_name: str,
    account_url: str = None,
    credential: object = None,
    conn_str: str = None,
    ):
    """Download an Azure blob object into memory"""
    if conn_str == None:
        if account_url is None or credential is None:
            logging.error('Both account url and credential must be provided!')
            return
        if not isinstance(credential, DefaultAzureCredential):
            logging.error('You must use a valid Azure credential')
        blob = BlobClient(
            account_url=account_url,
            credential=credential,
            container_name=container_name,
            blob_name=blob_name
        )
    else:
        if conn_str is None:
            logging.error('Connection string cannot be empty!')
            return
        blob = BlobClient.from_connection_string(
            conn_str=conn_str,
            container_name=container_name, 
            blob_name=blob_name)
    if blob.exists:     
        blob_data = blob.download_blob()
        blob_data = blob_data.readall()
        return blob_data
    else: 
        logging.error('Blob does not exist.')  

def save_to_blob(
        data: object, 
        container_name: str, 
        blob_name: str,
        account_url: str = None,
        credential: object = None, 
        conn_str: str = None, 
        verbose: bool = False, 
        **kwargs):

    """ Save to blob
    
    Saves a data object to an Azure blob storage.

    Args: 
        data: Data object
        container_name: Blob container 
        blob_name: Blob name
        account_url: Account URL. Use together with credential
        credential: Credential. Use together with account_url 
        conn_str: Connection string 
        verbose: If True additional information is printed to the console
        **kwargs: Additional arguments passed to upload_blob()

    """

    if verbose: logging.info('Connecting to blob...')
    if conn_str == None: 
        if account_url is None or credential is None:
            logging.error('Both account url and credential must be provided!')
            return
        if not isinstance(credential, DefaultAzureCredential):
            logging.error('You must use a valid Azure credential!')
        blob = BlobClient(
            account_url=account_url,
            credential=credential, 
            container_name=container_name,
            blob_name=blob_name)
    else: 
        if conn_str is None:
            logging.error('Connection string cannot be empty!')
            return        
        blob = BlobClient.from_connection_string(
            conn_str=conn_str,
            container_name=container_name,
            blob_name=blob_name)
    if blob.exists():
        logging.warn('Blob {0} already exists. Skipping upload'.format(blob_name))
    try: 
        if verbose: logging.info('Uploading blob...')
        blob.upload_blob(data, **kwargs) 
    except:
        logging.error('An error occured while trying to upload {0}'.format(blob_name))
