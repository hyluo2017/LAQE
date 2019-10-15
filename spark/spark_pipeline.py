
from pyspark.sql import SparkSession
import spark_transform as st
from pyspark.sql.functions import col
import spark_transform_v12 as st

def connect_db():

    url = "jdbc:xxxxxxxxx"
    properties={"user": "xxxxx", "password": "xxxxxx", "driver": "xxxxx"}
    return url, properties

def ETLpipeline(csvpath, zip_path, url, properties, spark):

    # transform the EPA source data with geohash and aggregation
    precision_level = 8
    aqsdf = st.aggdf(csvpath, precision_level, spark)

    # transform the zip-latlong csv source data with geohash
    zip_hashdf = st.hash_zip(zip_path, precision_level, spark)

    # cache the aqsdf and zip_hash as they will be used multiple times below
    # count() is an action, so that cache() is actively evaluated
    aqsdf.cache()
    aqsdf.count()
    zip_hashdf.cache()
    zip_hashdf.count()

    anchored_zip_list = []
    # transform zip-latlong source data, then join with epadf to find any matching
    # with geohash of high to low precision: k:v=precision_level:distance
    # distance: maximum distance between the air toxin sensor and the zip code
    # 3: '78km', 2: '630km', 8: '19m', 7: '76m'
    precision_levels = {6: '610m', 5: '2.4km', 4: '20km', 3: '78km'}

    for precision_level in [6,5,4,3]:

        distance = precision_levels.get(precision_level)

        # filter the zip codes that did not match any sensor at higher precision levels
        # i.e. small distance between sensor and zip code
        unanchored_zip_df = zip_hashdf.filter(~col("zip").isin(anchored_zip_list))

        result = st.joindf(aqsdf, unanchored_zip_df, precision_level, distance, spark)
        result.write.jdbc(url=url, table='tablexxxx', mode='append', properties=properties)

        # accumulate the zip codes that match with sensor in this loop
        anchored_zip_list += [i.zip for i in result.select('zip').distinct().collect()]

def main():
    # source
    bucket = 's3a://epa-aqs-insight/'
    # for filename in source:
    csvpath = bucket + 'hourly_V*.csv'
    zip_path = 's3a://aqs-bucket-insight/zip_latlong.csv'

    # sink
    url, properties = connect_db()

    # pipeline process and action
    spark = SparkSession.builder.getOrCreate()
    ETLpipeline(csvpath, zip_path, url, properties, spark)

if __name__ == '__main__':
    main()
