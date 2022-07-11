import os 
import dotenv
import geopandas
from rrutils import azure
from azure.identity import DefaultAzureCredential

# Load ENV vars 
dotenv.load_dotenv()

# Set globals 
account_url = os.environ['SVV_ACCOUNT_URL']
credential = DefaultAzureCredential()

def test_read_blob():
    r = azure.read_blob(
        blob_name = 'norway.json',
        container_name = 'ngcd-utils',
        credential = credential,
        account_url = account_url,
        function = geopandas.read_file)
    assert isinstance(r, geopandas.geodataframe.GeoDataFrame)
    
    

