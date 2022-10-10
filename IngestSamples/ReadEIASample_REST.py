#%%
# Example of reading from the Energy Information Administration (EIA).  This example
# requires an API key that is freely available with registration.
# API documentation is at https://www.eia.gov/opendata/documentation.php.

# You may need to install the following packages:
#   pip install requests
#   pip install pandas
import requests
import json
import pandas as pd

print("Imports complete")

#%%
# Get the average monthly price of electricity for MN since 2019 by sector.
with open("EIAAccountKey.config") as f:
    eia_key=f.readline()

request_params = {"api_key" : eia_key,
                    "start" : "2019-01",
                    "data[]" : "price",
                    "facets[stateid][][]" : "MN"}

api_response = requests.get(
    "https://api.eia.gov/v2/electricity/retail-sales/data/",
    params=request_params
)

print(api_response.content)

# %%
# Parse the result and convert the monthly price list to a Pandas dataframe.
json_data = json.loads(api_response.content)

response_json = json_data['response']  
total_data_points = response_json['total']
price_df = pd.DataFrame(response_json['data'])

print(total_data_points)
print(price_df)

    
# %%
