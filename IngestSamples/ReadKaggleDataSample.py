#%%
# Sample to read data from Kaggle.com using the Kaggle client library.

# You may need to install the following packages:
#   pip install kaggle
#   pip install pandas

# You will also need to store your credentials at the location described in
# https://github.com/Kaggle/kaggle-api#api-credentials
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd

print("Imports complete")

# %%
# Authentication defaults to use the config file in the predefined location.
api = KaggleApi()
api.authenticate()

# Get the competitions list to test out the API.
competitions = api.competitions_list()
print(competitions)

# %%
# Read data from a specific location.
dataset = 'uciml/iris'
out_path = 'datasets/iris'

api.dataset_download_file(dataset, 'Iris.csv', out_path)

# %%
# The location is specificed in the out_path in the previous cell.
file_name = ".\datasets\iris\iris.csv"

iris_df = pd.read_csv(file_name)

print(iris_df)



# %%
api.datasets_list()

# %%
