# %%
# EDASample - perform some basic analysis of a dataset.  We'll use the world_population.csv that was downloaded in ReadKaggleDataSample.py
import pandas as pd

pop_df = pd.read_csv("datasets/WorldPopulation/world_population.csv")

display(pop_df)

# %%
# Get data shape and data types for the columns.
print(pop_df.shape)

print(pop_df.dtypes)

# %%
# Look for missing data
print(pop_df.isna().sum())

# %%
# Look for duplicate rows.
print(pop_df.duplicated().sum())

#%%
# Continent looks like a categorical variable.  Find the unique values.
print(pop_df.Continent.unique())

# %%
# Find the count for each continent.
display(pop_df.groupby('Continent').count())

#%%
# Use pandas_profiling to create a more detailed report.
from pandas_profiling import ProfileReport

profile = ProfileReport(pop_df, title="Pandas Profiling Report")

profile.to_file('datasets/WorldPopulation/PopulationProfileReport.html')


