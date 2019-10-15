from flask import render_template, Response
from laqe import app
import pandas as pd
from flask import request
import laqe.queries_nonconf as Q

@app.route('/')
@app.route('/index')
def index():
   user = {'nickname': 'Lionel'}
   return render_template('index.html',
        title = 'Home',
        user = user)

@app.route('/db')
def airtoxin_page():
    sql_query = """
    SELECT zip, (100*year + month) as year_month, toxin FROM airtoxin WHERE zip = 'z19104' AND year >= {}
    """.format(2000)
    df = Q.read_db_test(sql_query)
    airtoxin = ''
    for i in range(0,30):
        airtoxin += df.iloc[i]['zip'] + str(df.iloc[i]['year_month']) + df.iloc[i]['toxin']
        airtoxin += '<br>'
    return airtoxin

@app.route('/laqe')
def airtoxins_input():
    return render_template("laqe.html")

@app.route('/output')
def airtoxins_output():

    # get inputs from user request
    homezip = request.args.get('zipcode1')
    workzip = request.args.get('zipcode2')

    startyear = int(request.args.get('start-year1'))
    startmonth = int(request.args.get('start-month1'))
    endyear = int(request.args.get('end-year1'))
    endmonth = int(request.args.get('end-month1'))
    starttime = request.args.get('start-year1') + '/' + request.args.get('start-month1')
    endtime = request.args.get('end-year1') + '/' + request.args.get('end-month1')

    # query DB with user inputs
    # then prepare the result into a list of dictionaries for rendering to output.html
    dbworkzip = 'z' + workzip
    dbhomezip = 'z' + homezip
    if workzip:
        p = Q.dayORnight_df(dbworkzip, True, startyear, startmonth, endyear, endmonth)
        q = Q.dayORnight_df(dbhomezip, False, startyear, startmonth, endyear, endmonth)
        daynote = 'Daytime air toxin information for zip code ' + workzip + ' from ' + starttime +' to '+ endtime+':'
        nightnote = 'Nighttime air toxin information for zip code ' + homezip + ' from '+starttime+' to '+endtime+':'
        result = [Q.result_list(q), nightnote, Q.result_list(p), daynote]
    else:
        q = Q.dayANDnight_df(dbhomezip, startyear, startmonth, endyear, endmonth)
        alldaynote = 'Air toxin information for zip code ' + homezip + ' from ' + starttime + ' to ' + endtime + ':'
        result = [Q.result_list(q), alldaynote]

    return render_template("output.html", result=result, workzip=workzip)