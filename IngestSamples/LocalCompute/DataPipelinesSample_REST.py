#%%
# REST API sample found on page 77 of "Data Pipelines Pocket Reference"
# Note that the exact api, iss-pass, is disabled.

# You may need to install the following packages:
#   pip install requests
#   pip install pandas
import requests
import json
import pandas as pd

print("Imports complete")


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

# Parse the result to JSON.
json_data = json.loads(api_response.content)

print(json_data)

#%%
# Convert the people list to a Pandas dataframe.
if json_data['message']=="success":
    astro_count = json_data['number']
    astros_df = pd.DataFrame(json_data['people'])

print("Total astronauts:  " + str(astro_count))
print(astros_df)

#%%
# Save astronaut list
astros_df.to_csv(".\ActiveAstronauts.csv", index=False)

# %%
