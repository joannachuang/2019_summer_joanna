
import pandas as pd
import csv
import numpy as np

def table_sheet2_parser(first_tb):
    second_tb = first_tb
    second_tb = pandas.dataframe()

    return second_tb

def table_sheet3_parser(first_tb):
    second_tb = first_tb
    second_tb = pandas.dataframe()

    return second_tb

for i in range(1,3): 
    url_part = '''http://163.29.251.188/botedata/%e4%ba%a4%e9%80%9a%e6%b5%81%e9%87%8f/107%e5%b9%b4%e5%ba%a6/htm/n{0}.files/sheet002.htm '''
    url = url_part.format("%03d"% i)
    tb = pd.read_html(url)

    
    df = first_tb = tb[0]
    first_tb = first_tb.drop(first_tb.index[:4],inplace = True)  # c與b的某些部分會被切掉????

    
    print (df[0])
    #print  (df.iloc[10].tolist())


    for key, row in df.iterrows():
        if len(row[0]) == 1 :
            #print (key, row[0], type(row[0]), len(row[0] ) )
            row = df.at[key][ 1:].tolist()
            df.at[key] = row
     
    print (df)
    
    #first_tb.to_csv('d:/mydata_{0}.csv'.format(i), encoding='big5', header=true, index=false)
    
    #print(  df.index[df[0].isalpha()] )

    
    print (tb[0])
    


    print(url_part,tb)

 
file1 = open('''d:/mydata_1.csv''', 'r').readlines()
fileout = open('mydata_1.csv', 'w')

for line in file1:
    if line and line[0].isalpha():
       line[0] == '0'

file1 = open('d:/mydata_1.csv', 'r').readlines()
fileout = open('d:/mydata_1.csv', 'w')
reader = csv.reader(file1, delimiter='\t')
for line in file1:
    for row in reader:
        if(row[0].isalpha()):
            fileout.write(line.replace(row[0], ''))
    print (line)
