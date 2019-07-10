
import Lily.ctao.database as cdb
import Lily.crawler.url_string as url
import requests
import pandas as pd
import numpy
import io
import json
import sqlite3
import os

df = pd.read_html('''https://taipeicity.github.io/traffic_realtime/''', header = 0)

#for i in range(0, len(df)):   
#    target = '''https://tcgbusfs.blob.core.windows.net/{0}.gz'''

#    link = df[i].filter(like='連結').columns
#    name = df[i].filter(regex='城市|說明').columns
#    df[i]['說明'] = df[i][name].apply(lambda x: ''.join(x), axis=1)

#    for j in range(len(df[i][link])):
#        target = target.format (df[i]['說明'].loc[j])  
#        file = '''d:/0702/{0}.gz'''.format(df[i]['說明'].loc[j])
#        arg1 =  url.url_string(target)
#        arg1.to_file(file)
#        print (target, file)


path = '''d:/0702'''
files = os.listdir(path)

for f in files:
    conn = sqlite3.connect("d:/CRAWLER_TASK.sqlite")
    cursor = conn.cursor()
    print ('Open database successfully')
    
    with open( f) as json_file:
        json_data = json.loads(json_file.read())  
        #get the list of the columns in the JSON file.
        columns = []
        column = []
        for data in json_data:
            column = list(data.keys())
            for col in column:
                if col not in columns:
                    columns.append(col)                               
        #get values of the columns in the JSON file in the right order.   
        value = []
        values = [] 
        for data in json_data:
            for i in columns:
                value.append(str(dict(data).get(i)))   
            values.append(list(value)) 
            value.clear()
            conn.close()

#print ('Complete database successfully')

    #cursor = conn.cursor() #建立一個游標查詢資料庫
    #cursor.execute('SELECT * FROM  youbike即時資訊')
    #data = cursor.fetchall()
    #for row in rows:
    #    print(row)
    #cursor.execute('''CREATE TABLE file *''')
    ##ret = cursor.fetchone()          
    #conn.commit() # 注意插入操作之後要進行提交
    ##for item in data:
    ##    print json.loads(item[0])
    #conn.close()


#for f in path:
#    conn = sqlite3.connect("d:/crawler_task.sqlite")
#    print ('open database successfully')
#    cursor = conn.cursor() #建立一個游標查詢資料庫
#    cursor.execute('select * from  youbike即時資訊')
#    data = cursor.fetchall()
#    for row in rows:
#        print(row)
#    cursor.execute('''create table file *''')
#    #ret = cursor.fetchone()          
#    conn.commit() # 注意插入操作之後要進行提交
#    #for item in data:
#    #    print json.loads(item[0])
#conn.close()
