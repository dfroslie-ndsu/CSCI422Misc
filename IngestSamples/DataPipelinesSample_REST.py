#%%
# REST API sample found on page 77 of "Data Pipelines Pocket Reference"
# Note that the exact api, iss-pass, is disabled.
import requests

# Get current space station position
api_response = requests.get(
    "http://api.open-notify.org/iss-now.json"
)

print(api_response.content)

# %%
import json
import pandas as pd

# Get current number of people in space
api_response = requests.get(
    "http://api.open-notify.org/astros.json" 
)

# Parse the result and convert the people list to a Pandas dataframe.
json_data = json.loads(api_response.content)

if json_data['message']=="success":
    astro_count = json_data['number']
    astros_df = pd.DataFrame(json_data['people'])

print(astros_df)

# %%
