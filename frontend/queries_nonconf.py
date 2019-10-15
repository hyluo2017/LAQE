import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2

user = 'nonconf'
host = 'nonconf'
dbname = 'nonconf'
db = create_engine('postgres://%s%s/%s'%(user, host, dbname))

con = None
con = psycopg2.connect(host=host, database = dbname, user=user, password='nonconf')

def read_db_test(sql_query):
    return pd.read_sql_query(sql_query, con)

def dayORnight_df(zip, dayORnight, start_year, start_month, end_year, end_month):
    lstart = 100 * start_year + start_month
    lend = 100 * end_year + end_month
    ORquery = """
            SELECT year, month, dayornight, toxin, average, unit, counts, distance
            FROM airtoxin
            WHERE zip = '{}' AND dayornight = {} AND  (100*year+month) >= {} AND (100*year+month) <= {} 
            """.format(zip, dayORnight, lstart, lend)
    return pd.read_sql_query(ORquery, con).drop_duplicates().sort_values(['year', 'month'], ascending=[False, False])

def dayANDnight_df(zip, start_year, start_month, end_year, end_month):

    lstart = 100 * start_year + start_month
    lend = 100 * end_year + end_month
    ANDquery = """
            SELECT year, month, toxin, average, unit, counts, distance
            FROM airtoxin 
            WHERE zip = '{}' AND  (100*year+month) >= {} AND (100*year+month) <= {} 
            """.format(zip, lstart, lend)
    result = pd.read_sql_query(ANDquery, con)
    result['sum'] = result['average']*result['counts']
    s = result.groupby(['year', 'month', 'toxin', 'unit', 'distance'], as_index=False).sum()
    s['average'] = s['sum']/s['counts']
    s = s[['year', 'month', 'toxin', 'average', 'unit', 'distance']].drop_duplicates()

    return s.sort_values(['year', 'month'], ascending=[False, False])

def result_list(q):

    resultlist = []
    for i in range(0,q.shape[0]):
        year = q.iloc[i]['year']
        month = q.iloc[i]["month"]
        toxin = q.iloc[i]['toxin']
        average = round(q.iloc[i]['average'],3)
        unit = q.iloc[i]['unit']
        distance = q.iloc[i]['distance']
        resultlist.append(dict(year=year, month=month, toxin=toxin, average=average, measure=unit, distance=distance))

    return resultlist
