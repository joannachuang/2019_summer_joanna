

import Lily.ctao.database as cdb
import Lily.crawler.url_string as url
import requests
import pandas as pd
import numpy
import io
import json
import sqlite3
import os
import xlrd
from xlutils.copy import copy
from xlwt import Style

df = pd.read_excel('''d:/pylily/0409/0409/data_opendata_tb_107lane_name.xls''', header = 0)


#for i in range(0, 236):   
#    target = '''https://maps.googleapis.com/maps/api/geocode/json?address={0}&key=AIzaSyAX1HCIvCMr_Cymu--zGH1uWoK5-S7qRjg'''

#    station_code = df.loc[:,'station_code']
#    station_name = df.loc[:,'station_name']
    
#    target  = target.format(station_name.loc[i])
#    filename  = station_code.loc[i]

#    file = '''C:/Users/User/Desktop/0710_road_latlng/{0}.json'''.format(filename)
#    arg1 =  url.url_string(target)
#    arg1.to_file(file)
#    print (target, file)

path = '''C:/Users/User/Desktop/0710_road_latlng'''
files = os.listdir(path)

px = []
py = []

for file in files:

    with open(path +'/'+ file , 'r', encoding='utf8') as reader:
        jf = json.loads(reader.read())
        # N016與另一個路口找不到點位   
        if not len(jf['results']) == 0:
            px.append( jf['results'][0]['geometry']['location']['lat']) 
            py.append( jf['results'][0]['geometry']['location']['lng']) 
        else :
            px.append('X')
            py.append('Y')
        
        #print(jf['results'][0]['geometry']['location']['lat']) if not len(jf['results']) == 0 else print('ZERO_RESULTS')
        #print(jf['results'][0]['geometry']['location']['lng']) if not len(jf['results']) == 0 else print('ZERO_RESULTS')

        #def get_px(file):
        #    if not len(jf['results']) == 0:
        #        file['px'] = jf['results'][0]['geometry']['location']['lat']
        #    else :
        #        file['px'] = 'ZERO_RESULTS'
        #def get_py(file):
        #    if not len(jf['results']) == 0:
        #        file['py'] = jf['results'][0]['geometry']['location']['lng']
        #    else :
        #        file['py'] = 'ZERO_RESULTS'
        #df = df.apply(get_px, axis = 0)
        #df = df.apply(get_py, axis = 0)

#print(px) 
#print(py)

df['px'] = px
df['py'] = py

#df['station_name_from'] =  df['station_name'].apply(lambda x : x.split('~')[0])
#df['station_name_to'] =  df['station_name'].apply(lambda x : x.split('~')[1])
new = df["station_name"].str.split("~", expand = True) 
df["station_1"]= new[0] 
df["station_2"]= new[1] 
df["station_3"]= new[2] if not new[2].empty else ''

cols = df.columns.tolist()
cols = cols[:2] + cols[-3:] + cols[3:9] + cols[10:12] + cols[9:10]
df = df[cols]


df.to_excel("0711_road_latlng_cp.xls", encoding='big5', header=True, index=False)