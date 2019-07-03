
import Lily.ctao.database as cdb
import Lily.crawler.url_string as url
import pandas as pd
from tabula import read_pdf
from tabula import convert_into
import csv
import numpy
import os
import xlrd
import xlsxwriter

df = read_pdf('''d:/107_taipei_traffic.pdf''', pages='1-7', pandas_options={'header': 0}, encoding='Big5', guess = False)

def table_sheet2_b(first_tb):
    second_tb = first_tb
    second_tb = pandas.Dataframe()

    return second_tb

def table_sheet3_parser(first_tb):
    second_tb = first_tb
    second_tb = pandas.Dataframe()

    return second_tb

path = '''d:/section_data/'''
files = os.listdir(path)

for i in range(1,24): 
    url_part = '''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/PDF/A{0}.pdf '''
    url = url_part.format("%03d"% i)
    tb = read_pdf(url)
    df = first_tb = tb

    
    first_tb['scooter1'] =  first_tb['機車'].apply(lambda x : x.split(' ')[0])
    first_tb['scooter2'] =  first_tb['機車'].apply(lambda x : x.split(' ')[1])

    cols = first_tb.columns.tolist()
    cols = cols[:6] + cols[-2:] + cols[7:-2]

    first_tb = first_tb[cols]
    
    first_tb.to_csv('d:/section_data/pce_{0}.csv'.format("%03d"%i), encoding='big5', header=True, index=False)

    print(first_tb['機車'])

#source_csv = ['''d:/section_data/pce_006.CSV''','''d:/section_data/pce_007.CSV''','''d:/section_data/pce_008.CSV''','''d:/section_data/pce_009.CSV''','''d:/section_data/pce_022.CSV''']
#target_csv = '''d:/section_data/pce_banqiao.CSV'''

#data = []
#for i in source_csv:
#    wb = xlrd.open_workbook(i)
#    for sheet in wb.sheets():
#        for rownum in range(sheet.nrows): 
#            data.append(sheet.row_values(rownum))
#print (data)

workbook = xlsxwriter.Workbook(target_csv)
worksheet = workbook.add_worksheet()
font = workbook.add_format({"font_size":12})
for i in range(len(data)):
    for j in range((data[i])):
        worksheet.write(i,j,data[i][j],font)
workbook.close()