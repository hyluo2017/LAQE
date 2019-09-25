# anchor every zip code with the closest hap sensor location
import pandas as pd
import os
from datetime import datetime

# anchoring the specific zip code box with a point in aqs_df
# condition: the point in aqs_df falls in the zip box
def fetch_anchor_for_zip_box(zip_box, aqs_df):

    box_s = zip_box[0]
    box_n = zip_box[1]
    box_w = zip_box[2]
    box_e = zip_box[3]

    anchor = []
    for i in range(aqs_df.shape[0]):
        lata = aqs_df.iloc[i][0]
        longa = aqs_df.iloc[i][1]
        if (box_s < lata < box_n) & (box_w < longa < box_e):
            anchor = [lata, longa]

    return anchor

# define the square box that centers at the zipcode lat-long
def set_zip_laglong_box(lati, longi, box_side):
    lat_s = lati - box_side
    lat_n = lati + box_side
    long_w = longi - box_side
    long_e = longi + box_side

    return (lat_s, lat_n, long_w, long_e)

# anchor all zip codes with points in aqs_df
def anchor_zips(zip_df, aqs_df):

    # for each zipcode, find the anchor AQS sensor's lat-long
    anchor_list = []
    for i in range(zip_df.shape[0]):
        zipcode = zip_df.iloc[i]['Zip']
        lati = zip_df.iloc[i]['geocenter_lat']
        longi = zip_df.iloc[i]['geocenter_long']

        # lat/long precision of 0.001 equals ~ 111 m
        box_side = 0.001
        anchor = []
        while len(anchor) == 0:
            zip_box = set_zip_laglong_box(lati, longi, box_side)
            anchor = fetch_anchor_for_zip_box(zip_box, aqs_df)
            if len(anchor) != 0:
                anchor.insert(0, zipcode)
                anchor_list.append(anchor)
            box_side *= 2

    return anchor_list

def main():
    # 1 read in zip and asq lat-long files
    zip_df = pd.read_csv('test3/zip_latlong.csv')
    aqs_df = pd.read_csv('../dataset/hap_hourly_summary000000000000.csv')
    # aqs_df = pd.read_csv('test3/aqs_latlong.csv') # this is for test3
    aqs_df = aqs_df[['latitude', 'longitude']].drop_duplicates()
    anchor_list = anchor_zips(zip_df, aqs_df)
    result = pd.DataFrame(anchor_list, columns=['zipcode', 'anchor_lat', 'anchor_long'])
    result.to_csv('test4/result.csv')

if __name__ == '__main__':

    start = datetime.now()
    main()
    runtime = datetime.now() - start
    print('program finished in {} seconds!'.format(runtime.total_seconds()))