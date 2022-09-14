#%%
# This assumes that DataPipelinesSample_REST.py has been executed
# and a local file, ActiveAstronauts.csv exists.

# You may need to install the following packages:
#   pip install requests
#   pip install pandas
import requests
import json
import pandas as pd

print("Hello world")  

#%%
# Get current space station position
api_response = requests.get(
    "http://api.open-notify.org/iss-now.json"
)

print(api_response.content)

# %%
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


