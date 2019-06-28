import Lily.ctao.database as cdb
import Lily.crawler.url_string as url


for a in range(1,10):
    target='''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S00{0}.files/sheet001.htm'''
    target = target.format(a)
    file  = '''D:/107_s00{0}_0001.html'''
    file  = file.format(a)
    arg1 =  url.url_string(target)
    arg1.to_file(file)
    print (target, file)

for a in range(1,10):
    target='''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S0{0}0.files/sheet001.htm'''
    target = target.format(a)
    file  = '''D:/107_s0{0}0_0001.html'''
    file  = file.format(a)
    arg1 =  url.url_string(target)
    arg1.to_file(file)
    print (target, file)