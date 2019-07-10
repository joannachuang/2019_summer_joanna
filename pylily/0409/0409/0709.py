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
    db = cdb.database('data_crawler.sqlite')

    sql = '''
        select Id, routeId,  nameZh, nameEn, seqNo, pgp, longitude, showLon, showLat, vector from  group by id
    '''
    df = pandas.read_sql(sql, db.connect , index_col=['rowid'])

    df = df.reindex ( columns=[ 'a','b','c', 'd', 'e', 'f', 'g', 'h','i' ], fill_value=''  )

    for ind, row in df.iterrows():
#        print ('melt', row[0])
        json_tables = pandas.read_html( lzma.decompress( sqlite3.Binary(row['lzma_html']) ), encoding='utf-8')
        arg2 = json_tables[2]
        df.at[ind,'routeId'] = arg2.iat[0,1] 
        df.at[ind,'nameZh'] = arg2.iat[1,1]
        df.at[ind,'nameEn'] = arg2.iat[2,1]
        df.at[ind,'seqNo'] = arg2.iat[3,1]
        df.at[ind,'pgp'] = arg2.iat[4,1]
        df.at[ind,'longitude'] = arg2.iat[5,1]

        for ind2, row2 in json_tables[3].iterrows():
            if isinstance(row2, pandas.core.series.Series):
                for elem in row2:
                    if isinstance(elem,str):
                        df.at[ind, 'i'] = df.at[ind, 'i'] + ',' + elem 
            else:
                if isinstance(elem,str):
                    df.at[ind, 'i'] = df.at[ind, 'i'] + ',' + row2

    #df = df.drop(columns=['lzma_html'])
    df.to_sql('data_rdset_pylily', db.connect, if_exists='append', index=False)

    db.connect.execute('''delete from {0} where rowid not in 
                         (select max (rowid) from {0} group by id)'''.format('data_rdset_pylily') )
    
    db.connect.commit()
    return

def cwb_melt2():
    db = cdb.database('data_crawler.sqlite')
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
    df2 = pandas.DataFrame(station, columns=['a','b','c','d', 'e','f','g', 'h', 'i'])
    df2.to_sql('data_rdset_pylily_cwb_sensible_earthquake_LocalSeismicIntensity', db.connect, if_exists='replace', index=False)
    return

if __name__ == '__console__' or __name__ == '__main__':
    import os
    thost = chmd.hostmetadata()
    os.chdir (thost.warehouse)
    cwb_crawler()
    cwb_melt1()
    cwb_melt2()
