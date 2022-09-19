#%%
# Sample to write CSV content to Azure Data Lake Storage.

# This assumes that DataPipelinesSample_REST.py has been executed
# and a local file, ActiveAstronauts.csv exists.

# You may need to install the following packages:
#   pip install pandas
#   pip install azure-storage-file-datalake
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

print("Imports complete")  


# %%
# Method to connect to a storage account with an account key.
# For this and other account connection approaches, see https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-python
def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)


#%%
storage_account_name = "assign1storage"

# Note - store file in a config file that is NOT checked in.
# Use .gitignore to avoid accidental submission.
with open("AccountKey.config") as f:
    storage_account_key=f.readline()

initialize_storage_account(storage_account_name, storage_account_key)

print(service_client)

#%%
# Create a container and a directory.
file_system_client = service_client.create_file_system(file_system="assign-2-sample2")    

directory_client=file_system_client.create_directory("Astronauts2")


#%%
# Upload the data file.
directory_client = file_system_client.get_directory_client("Astronauts")
        
file_client = directory_client.create_file("ActiveAstronauts.csv")

astronauts_file = open("ActiveAstronauts.csv",'r')

file_contents = astronauts_file.read()

file_client.upload_data(file_contents, overwrite=True)

