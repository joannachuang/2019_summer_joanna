import pandas as pd
import csv
import numpy
import os

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
    if i.endswith("sheet002.htm"):
        df = pd.read_html(i)  #cannot find table, but it had surely downloaded the file 
        first_tb = tb[0]
        first_tb.to_csv('d:/mydata_{0}.csv'.format(i), encoding='big5', header=True, index=False)
        print(url_part,tb)