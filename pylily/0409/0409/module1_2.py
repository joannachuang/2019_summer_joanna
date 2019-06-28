import Lily.ctao.database as cdb
import Lily.crawler.url_string as url
import pandas as pd
from tabula import read_pdf
import csv
import numpy
import os

df = read_pdf('''d:/107_taipei_traffic.pdf''', pages='1-7', pandas_options={'header': 0}, encoding='Big5', guess = False)
#java問題，Error: Error occurred during initialization of VM
#Corrupted ZIP library: C:\OSGeo4W64\bin\zip.dll
#Command '['java', '-jar', 'C:\\OSGeo4W64\\apps\\Python37\\lib\\site-packages\\tabula\\tabula-1.0.2-jar-with-dependencies.jar', '--pages', '1-7', 'd:/107_taipei_traffic.pdf']' returned non-zero exit status 1.


for ind, row in df.iterrows():
    target = '''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/PDF/{0}.pdf'''
    target = target.format (row[0])
    file = '''d:/{0}.pdf'''.format(row[0])
    arg1 =  url.url_string(target)
    arg1.to_file(file)
    print (target, file)

for ind, row in df.iterrows():
    target = '''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/{0}.htm'''
    target = target.format (row[0])
    file = '''d:/{0}.htm'''.format(row[0])
    arg1 =  url.url_string(target)
    arg1.to_file(file)
    print (target, file)

read_html to_csv
def table_sheet2_b(first_tb):
    second_tb = first_tb
    second_tb = pandas.Dataframe()

    return second_tb

def table_sheet3_parser(first_tb):
    second_tb = first_tb
    second_tb = pandas.Dataframe()

    return second_tb

path = '''d:/'''
files = os.listdir(path)

for i in files:
    if i.endswith("_sheet002.htm"):
        df = pd.read_html(path+i)  # notice "path+i"
        first_df = df[0]
        first_df.to_csv('d:/mydata_{0}.csv'.format(i), encoding='big5', header=True, index=False)
        first_tb = first_tb.drop(first_tb.index[:4],inplace = True)
        print(first_df)

