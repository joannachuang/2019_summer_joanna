import json
import sqlite3
from datetime import datetime
import os

import Lily.ctao.database as cdb
import Lily.crawler.url_string as url
import requests
import pandas as pd
import numpy
import io



df = pd.read_html('''https://taipeicity.github.io/traffic_realtime/''', header = 0)

#for i in range(0, len(df)):   
#    target = '''https://tcgbusfs.blob.core.windows.net/{0}.gz'''

#    link = df[i].filter(like='連結').columns
#    name = df[i].filter(regex='城市|說明').columns
#    df[i]['說明'] = df[i][name].apply(lambda x: ''.join(x), axis=1)

#    for j in range(len(df[i][link])):
#        if "ntpcbus" not in target: 
#            target = target.format (df[i]['說明'].loc[j])  
#            file = '''d:/0702/newtaipei/{0}.gz'''.format(df[i]['說明'].loc[j])
#            arg1 =  url.url_string(target)
#            arg1.to_file(file)
#            print (target, file)

#        else :
#            target = target.format (df[i]['說明'].loc[j])  
#            file = '''d:/0702/taipei/{0}.gz'''.format(df[i]['說明'].loc[j])
#            arg1 =  url.url_string(target)
#            arg1.to_file(file)
#            print (target, file)



## 試著將一個json檔匯入

#path = '''d:/0702'''
#files = os.listdir(path)
#conn = sqlite3.connect("d:/CRAWLER_TASK.sqlite")
#cursor = conn.cursor()
#print ('Open database successfully')
#with open( '''d:/0702/臺北市站牌.gz''', encoding='utf-8-sig') as json_file:
#    json_data = json.loads(json_file.read()) #傳遞文件內容  
#print ('Complete database successfully')

def cwb_melt1():
    import lzma
    db = cdb.database('data_crawler.sqlite')

    sql = '''
        select Id, routeId,  nameZh, nameEn, seqNo, pgp, longitude, showLon, showLat, vector from  'd:/0702/臺北市站牌.gz' group by id
    '''
    df = pandas.read_sql(sql, db.connect , index_col=['routeId'])

    df = df.reindex ( columns=[ 'Id','routeId','nameZh', 'nameEn', 'seqNo', 'pgp', 'longitude', 'showLon','showLat','vector' ], fill_value=''  )

    for ind, row in df.iterrows():
#        print ('melt', row[0])
        arg2 = json_tables[0]
        df.at[ind,'routeId'] = arg2.iat[0,1] 
        df.at[ind,'nameZh'] = arg2.iat[1,1]
        df.at[ind,'nameEn'] = arg2.iat[2,1]
        df.at[ind,'seqNo'] = arg2.iat[3,1]
        df.at[ind,'pgp'] = arg2.iat[4,1]
        df.at[ind,'longitude'] = arg2.iat[5,1]
        df.at[ind,'showLon'] = arg2.iat[6,1]
        df.at[ind,'showLat'] = arg2.iat[7,1]
        df.at[ind,'vector'] = arg2.iat[8,1]

        for ind2, row2 in json_tables[3].iterrows():
            if isinstance(row2, pandas.core.series.Series):
                for elem in row2:
                    if isinstance(elem,str):
                        df.at[ind, 'i'] = df.at[ind, 'i'] + ',' + elem 
            else:
                if isinstance(elem,str):
                    df.at[ind, 'i'] = df.at[ind, 'i'] + ',' + row2

    df.to_sql('data_rdset_pylily', db.connect, if_exists='append', index=False)

    db.connect.execute('''delete from {0} where rowid not in 
                         (select max (rowid) from {0} group by id)'''.format('data_rdset_pylily') )
    
    db.connect.commit()
    return