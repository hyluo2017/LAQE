# this script is to prepare input datasets for test1

import pandas as pd

# Error will occur due to file path issue
# DtypeWarning: Columns (17) have mixed types. Specify dtype option on import or set low_memory=False.
# Column 17 is "uncertainty", and is empty in hap_hourly.csv of test case #1 below
# df.shape: (1452451, 24); took about 6 sec to read in
df = pd.read_csv('../dataset/hap_hourly_summary000000000000.csv')

# sample the df every 145000 rows
# ef.shape: (11, 24)
ef = df.iloc[::145000, :]
ef = ef[['site_num', 'date_gmt', 'time_gmt', 'parameter_name',
             'sample_measurement', 'units_of_measure']]
ef.to_csv('test1/hap_hourly.csv')
