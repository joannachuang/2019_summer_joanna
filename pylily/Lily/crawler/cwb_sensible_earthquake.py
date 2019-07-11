# -*- coding: utf-8 -*-
import re
import io
import time
import numpy
import pandas
import sqlite3
import datetime
import requests
import lxml.html as LH
import xml.etree.ElementTree as xmltree
import Lily.ctao.hostmetadata as chmd
import Lily.ctao.database as cdb

def cwb_crawler():

    import Lily.crawler.url_string as  curlstr
    db = cdb.database('data_crawler_cwb_earthquake_list.sqlite')

    db_target_tab   = 'data_crawler_cwb_sensible_earthquake_download'

    eq_dir_pa  = '''https://scweb.cwb.gov.tw/earthquake/Page.aspx?ItemId=20&Date={0}'''
    sub_url    = '''https://scweb.cwb.gov.tw/earthquake/Page.aspx{0}'''
    days      = pandas.date_range(datetime.datetime.today() - datetime.timedelta(days=90), 
                                  datetime.datetime.today() + datetime.timedelta(days=31), 
                                  freq='M')
    
    for mon in days:

        eq_list = pandas.DataFrame(columns=['id', 'lzma_html', 'download_time'])
        time.sleep (90/1000.0)

        day_url = eq_dir_pa.format( mon.strftime('%Y%m') )
        cur1 = curlstr.url_string (day_url) 
        arg1 = LH.fromstring( cur1.to_str() )
        arg2 = arg1.xpath('//tr/td/a/@href')
        arg3 = {}

        for elem in arg2:
            if elem not in arg3  and elem[1:7] == 'ItemId': 
                print ('download html', elem)
                cur2 = curlstr.url_string (sub_url.format(elem))
                arg3[elem] = True           
                eq_list.loc[ len(eq_list)] = [elem[22:], cur2.to_lzma_xz() , datetime.datetime.now() ]

        eq_list.to_sql(db_target_tab, db.connect, if_exists='append' , index=False)

    db.connect.execute('''delete from {0} where rowid not in 
                         (select max (rowid) from {0} group by id, lzma_html)'''.format(db_target_tab) )
    
    db.connect.commit()
    return

def cwb_melt0():
    import lzma
    #db = cdb.database('data_crawler_cwb_earthquake_list.sqlite')
    #df = db.to_dataframe('data_crawler_cwb_earthquake_list')    
    #df = df.reindex ( columns=['id', 'html', 'lzma_html'], fill_value='' )

    #for ind, row in df.iterrows():
    #    df.at[ind,'lzma_html'] = lzma.compress(df.at[ind,'html'])
    #    print (df.at[ind,'id'] )

    #df = df.drop(columns=['html'])
    #df.to_sql('data_crawler_cwb_sensible_earthquake_download', db.connect, if_exists='replace', index=False)
    return

def cwb_melt1():
    import lzma
    db = cdb.database('data_crawler_cwb_earthquake_list.sqlite')

    sql = '''
        select max(rowid) rowid, id,  lzma_html from data_crawler_cwb_sensible_earthquake_download group by id
    '''
    df = pandas.read_sql(sql, db.connect , index_col=['rowid'])

    df = df.reindex ( columns=['id', 'lzma_html','time','py', 'px', 'depth', 'ML', 'Location', 'Stations' ], fill_value=''  )

    for ind, row in df.iterrows():
#        print ('melt', row[0])
        html_tables = pandas.read_html( lzma.decompress( sqlite3.Binary(row['lzma_html']) ), encoding='utf-8')
        arg2 = html_tables[2]
        df.at[ind,'time'] = arg2.iat[0,1] 
        df.at[ind,'py'] = arg2.iat[1,1]
        df.at[ind,'px'] = arg2.iat[2,1]
        df.at[ind,'depth'] = arg2.iat[3,1]
        df.at[ind,'ML'] = arg2.iat[4,1]
        df.at[ind,'Location'] = arg2.iat[5,1]

        for ind2, row2 in html_tables[3].iterrows():
            if isinstance(row2, pandas.core.series.Series):
                for elem in row2:
                    if isinstance(elem,str):
                        df.at[ind, 'Stations'] = df.at[ind, 'Stations'] + ';' + elem 
            else:
                if isinstance(elem,str):
                    df.at[ind, 'Stations'] = df.at[ind, 'Stations'] + ';' + row2

    df = df.drop(columns=['lzma_html'])
    df.to_sql('data_rdset_pylily_cwb_sensible_earthquake', db.connect, if_exists='append', index=False)

    db.connect.execute('''delete from {0} where rowid not in 
                         (select max (rowid) from {0} group by id)'''.format('data_rdset_pylily_cwb_sensible_earthquake') )
    
    db.connect.commit()
    return

def cwb_melt2():
    db = cdb.database('data_crawler_cwb_earthquake_list.sqlite')
    df = db.to_dataframe('data_rdset_pylily_cwb_sensible_earthquake')    

    station = []
    for ind, row in df.iterrows():

        for st in df.at[ind, 'Stations'].split(';'):
            if u'''地區最大震度''' not in st and st != '':
                rdset = [df.at[ind, 'id'], 
                         df.at[ind, 'time'],
                         float(df.at[ind, 'px'][4:-2]),
                         float(df.at[ind, 'py'][4:-2]),
                         float(df.at[ind, 'depth'][:-3]),
                         float(df.at[ind, 'ML']),
                         df.at[ind, 'Location'],
                         ''.join(st.split('\u3000')[:-1]) , float(st.split('\u3000')[-1:][0]) ]
                station.append(rdset)
    df2 = pandas.DataFrame(station, columns=['id','time','px','py', 'depth','ML','Location', 'Station', 'SeismicIntensity'])
    df2.to_sql('data_rdset_pylily_cwb_sensible_earthquake_LocalSeismicIntensity', db.connect, if_exists='replace', index=False)
    return

if __name__ == '__console__' or __name__ == '__main__':
    import os
    thost = chmd.hostmetadata()
    os.chdir (thost.warehouse)
    cwb_crawler()
    cwb_melt1()
    cwb_melt2()