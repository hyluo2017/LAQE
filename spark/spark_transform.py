import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import collect_list, sum, count, substring, expr, lit, col
import pygeohash as gh

# preprocess original dataset by parsing, geohashing and aggregation
def aggdf(csvpath, precision_level, spark):

    # user defined function for geohashing
    udf_gh = F.udf(lambda x, y: gh.encode(x, y, precision=precision_level))

    # parsing and geohashing
    df = spark.read.format("csv").option("header", "true")\
        .load(csvpath)\
        .select(
        udf_gh(F.col('Latitude').cast('float'), F.col('Longitude').cast('float')).alias('aqsgeohash'),
        F.year('Date Local').alias('year'),
        F.month('Date Local').alias('month'),
        ((7 <= F.hour('Time Local')) & (F.hour('Time Local') < 19)).alias('dayORnight'),
        F.col('Parameter Name').alias('toxin'),
        F.col('Units of Measure').alias('unit'),
        F.col('Sample Measurement').cast('float').alias('reading')
    )

    # aggregate the aqs sensor dataset by location, time, toxin, and unit
    grouping_cols = ['aqsgeohash', 'year', 'month', 'dayORnight', 'toxin', 'unit']
    df = df.groupBy(grouping_cols).agg(sum('reading').alias('sum'), count('reading').alias('counts'))

    return df

def hash_zip(zip_path, precision_level, spark):

    # geohash precision(+/-): 8(19m), 7(76m), 6(0.61km), 5(2.4km), 4(20km), 3(78km), 2(630km)
    udf_gh = F.udf(lambda x, y: gh.encode(x, y, precision=precision_level))

    ziphash_df = spark.read.format("csv").option("header", "true").load(zip_path)\
        .select(
        F.col('zip'),
        udf_gh(F.col('latitude').cast('float'), F.col('longitude').cast('float')).alias('zipgeohash')
    )

    return ziphash_df

def joindf(aqsdf, zipdf, precision_level, distance, spark):

    # join the zipcode and preprocessed (EPA)AQS dataset by precision level of geohashing
    aqsdf = aqsdf.withColumn('aqsdf_hashsub', aqsdf.aqsgeohash.substr(1, precision_level))
    zipdf = zipdf.withColumn('zipdf_hashsub', zipdf.zipgeohash.substr(1, precision_level))

    # small table of zipcode is left joined with large table of preprocessed AQS dataset
    joinType = 'left'
    joinExpression = zipdf.zipdf_hashsub == aqsdf.aqsdf_hashsub
    joinresult = zipdf.join(aqsdf, joinExpression, joinType)\
        .select(
        'zip', 'year', 'month', 'dayORnight', 'toxin', 'sum', 'unit', 'counts'
    )\
        .filter(col('sum').isNotNull())

    # ready the joined dataframe for output into database sink
    result = joinresult.groupBy('zip', 'year', 'month', 'dayORnight', 'toxin', 'unit')\
        .agg(sum('sum').alias('sum'), sum('counts').alias('counts'))\
        .withColumn('average', col('sum')/col('counts'))\
        .withColumn('distance', lit(distance))\
        .select(
        'zip', 'year', 'month', 'dayORnight', 'toxin', 'average', 'unit', 'counts', 'distance'
    )

    return result
