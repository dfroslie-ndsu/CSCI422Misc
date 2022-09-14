#%%
# This assumes that DataPipelinesSample_REST.py has been executed
# and a local file, ActiveAstronauts.csv exists.

# You may need to install the following packages:
#   pip install pandas
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

print("Hello world")  

#%%
# Get current space station position
astros_df = pd.read_csv("ActiveAstronauts.csv")

print(astros_df)

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






# %%
