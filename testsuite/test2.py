# this script is to prepare input datasets for test2
# test2 includes all data in all data in hap_hourly_summary000000000000.csv' (1452451 rows)

import pandas as pd

# Error will occur due to file path issue
# DtypeWarning: Columns (17) have mixed types. Specify dtype option on import or set low_memory=False.
# Column 17 is "uncertainty", and is empty in hap_hourly.csv of test case #1 below
# df.shape: (1452451, 24); took about 6 sec to read in
df = pd.read_csv('../dataset/hap_hourly_summary000000000000.csv')

# sample the df every 145000 rows
# ef = df.iloc[::145000, :] # test1 --> ef.shape: (11, 24)

# ef = df.iloc[::145, :] # test1.5 --> ef.shape: (10017, 24)

ef = df[['site_num', 'date_gmt', 'time_gmt', 'parameter_name',
             'sample_measurement', 'units_of_measure']]
ef.to_csv('test2/hap_hourly.csv')

print(ef.shape)
