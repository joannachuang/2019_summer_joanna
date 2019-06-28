import Lily.ctao.database as cdb
import Lily.crawler.url_string as url
import pandas
import datetime

#給日期物件
days      = pandas.date_range(datetime.datetime.strptime('20190423', '%Y%m%d'),   #datetime使用法+給一個format百分比代表格式
                              datetime.datetime.strptime('20190621', '%Y%m%d'), freq='D')   # D代表以天為單位，可用以做萬年曆的加減

#把物件納入迴圈使用
for d in days:
    target = '''http://163.29.3.98/XML/{0}.zip'''   #{0}以日期為字串所形成的檔名
    target = target.format(d.strftime('%Y%m%d'))    
    file = '''d:/{0}.zip'''.format(d.strftime('%Y%m%d'))
    arg1 =  url.url_string(target)
    arg1.to_file(file)
    print (target, file)

for a in range(1,10):
    target='''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S00{0}.files/sheet001.htm'''
    target = target.format(a)
    file  = '''d:/107_s00{0}_0001.html'''
    file  = file.format(a)
    arg1 =  url.url_string(target)
    arg1.to_file(file)
    print (target, file)