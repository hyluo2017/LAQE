import pandas as pd
from datetime import datetime

# clean up the source dataset of zipcode -lat/long (geocenter)
def zip_latlong_geohash():
    filename = "dataset/us-zip-code-latitude-and-longitude.csv"
    df = pd.read_csv(filename, delimiter=';')[['Zip', 'geopoint']]

    data = []
    df['Zip'] = df['Zip'].astype(int)

    # a few bad data points with zip code like 2xx, 5xx, with lat-long not correct
    # in addition, 222 items are three digits, missing two leading 0
    # in addition, 3215 items have four digits, missing one leading 0
    for i in range(df.shape[0]):
        geopoint = df.iloc[i][1].split(',')
        try:
            zipcode = 'z' + str(int(df.iloc[i][0]))
            if len(zipcode) == 5:
                zipcode = zipcode[:1] + '0' + zipcode[1:]
                alist = [zipcode, float(geopoint[0]), float(geopoint[1])]
                data.append(alist)
            elif len(zipcode) == 4:
                if ('z2' in zipcode) | ('z5' in zipcode):
                    pass
                else:
                    zipcode = zipcode[:1] + '00' + zipcode[1:]
                    alist = [zipcode, float(geopoint[0]), float(geopoint[1])]
                    data.append(alist)
            else:
                alist = [zipcode, float(geopoint[0]), float(geopoint[1])]
                data.append(alist)
        except:
            pass

    ef = pd.DataFrame(data, columns=['zip', 'latitude', 'longitude'])
    # ef = ef.iloc[::4000, :]
    # ef['geohash'] = ef.apply(lambda x: gh.encode(x.latitude, x.longitude, precision=7), axis=1)
    ef.to_csv('dataset/zip_latlong.csv')
    return "Done!"


zip_latlong_geohash()
