import Lily.ctao.database as cdb
import Lily.crawler.url_string as url
import pandas
import numpy
import requests
import os
import io

def write_url_tofile(url, filename):
    response = requests.get(url , verify=False)
    
    datafile = io.BytesIO()        
    for chunk in response.iter_content(chunk_size=512 * 1024): 
        if chunk: # filter out keep-alive new chunks
            datafile.write(chunk)

    f = open (filename,'wb')
    f.write(datafile.getvalue())
    f.close()
    return datafile.getvalue()

for a in range(1,120):
    target='''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/PDF/N{0}.pdf'''
    target = target.format("%03d" % a)
    file  = '''d:/107_N{0}.pdf'''
    file  = file.format("%03d" % a)
    arg1 =  url.url_string(target)
    arg1.to_file(file)
    print (target, file)

# delete file 
os.remove('''d:/107_N012.pdf''')
os.remove('''d:/107_N105.pdf''')

for b in range(1,115):
    target='''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/PDF/S{0}.pdf'''
    target = target.format("%03d" % b)
    file  = '''d:/107_S{0}.pdf'''
    file  = file.format("%03d" % b)
    arg1 =  url.url_string(target)
    arg1.to_file(file)
    print (target, file)

os.remove('''d:/107_S024.pdf''')



for b in range(1,120):
  for c in range(1,6):
    target='''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N{0}.files/sheet{1}.htm'''
    target = target.format("%03d"%b,"%03d"%c)
    file  = '''d:/107_N{0}_sheet{1}.htm'''
    file  = file.format("%03d"%b,"%03d"%c)
    arg1 =  url.url_string(target)
    arg1.to_file(file)
    print (target, file)

for d in range(1,115):
  for e in range(1,6):
    target='''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S{0}.files/sheet{1}.htm'''
    target = target.format("%03d"%d,"%03d"%e)
    file  = '''d:/107_S{0}_sheet{1}.htm'''
    file  = file.format("%03d"%d,"%03d"%e)
    arg1 =  url.url_string(target)
    arg1.to_file(file)
    print (target, file)


os.remove('''d:/107_N012_sheet001.htm''')
os.remove('''d:/107_N012_sheet002.htm''')
os.remove('''d:/107_N012_sheet003.htm''')
os.remove('''d:/107_N012_sheet004.htm''')
os.remove('''d:/107_N012_sheet005.htm''')
os.remove('''d:/107_N105_sheet001.htm''')
os.remove('''d:/107_N105_sheet002.htm''')
os.remove('''d:/107_N105_sheet003.htm''')
os.remove('''d:/107_N105_sheet004.htm''')
os.remove('''d:/107_N105_sheet005.htm''')
os.remove('''d:/107_S024_sheet001.htm''')
os.remove('''d:/107_S024_sheet002.htm''')
os.remove('''d:/107_S024_sheet003.htm''')
os.remove('''d:/107_S024_sheet004.htm''')
os.remove('''d:/107_S024_sheet005.htm''')

if __name__ == '__console__' or __name__ == '__main__':
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/PDF/N012(1).pdf''', 'd:/107_N012(1).pdf')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/PDF/N012(2).pdf''', 'd:/107_N012(2).pdf')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/PDF/N105(1).pdf''', 'd:/107_N105(1).pdf')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/PDF/N105(2).pdf''', 'd:/107_N105(2).pdf')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/PDF/S024(1).pdf''', 'd:/107_S024(1).pdf')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/PDF/S024(2).pdf''', 'd:/107_S024(2).pdf') 

    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N012(1).files/sheet001.htm''', 'd:/107_N012(1)_sheet001.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N012(1).files/sheet002.htm''', 'd:/107_N012(1)_sheet002.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N012(1).files/sheet003.htm''', 'd:/107_N012(1)_sheet003.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N012(1).files/sheet004.htm''', 'd:/107_N012(1)_sheet004.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N012(1).files/sheet005.htm''', 'd:/107_N012(1)_sheet005.htm')

    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N012(2).files/sheet001.htm''', 'd:/107_N012(2)_sheet001.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N012(2).files/sheet002.htm''', 'd:/107_N012(2)_sheet002.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N012(2).files/sheet003.htm''', 'd:/107_N012(2)_sheet003.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N012(2).files/sheet004.htm''', 'd:/107_N012(2)_sheet004.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N012(2).files/sheet005.htm''', 'd:/107_N012(2)_sheet005.htm')

    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N105(1).files/sheet001.htm''', 'd:/107_N105(1)_sheet001.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N105(1).files/sheet002.htm''', 'd:/107_N105(1)_sheet002.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N105(1).files/sheet003.htm''', 'd:/107_N105(1)_sheet003.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N105(1).files/sheet004.htm''', 'd:/107_N105(1)_sheet004.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N105(1).files/sheet005.htm''', 'd:/107_N015(1)_sheet005.htm')

    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N105(2).files/sheet001.htm''', 'd:/107_N105(2)_sheet001.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N105(2).files/sheet002.htm''', 'd:/107_N105(2)_sheet002.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N105(2).files/sheet003.htm''', 'd:/107_N105(2)_sheet003.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N105(2).files/sheet004.htm''', 'd:/107_N105(2)_sheet004.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/N105(2).files/sheet005.htm''', 'd:/107_N105(2)_sheet005.htm') 
    
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S024(1).files/sheet001.htm''', 'd:/107_S024(1)_sheet001.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S024(1).files/sheet002.htm''', 'd:/107_S024(1)_sheet002.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S024(1).files/sheet003.htm''', 'd:/107_S024(1)_sheet003.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S024(1).files/sheet004.htm''', 'd:/107_S024(1)_sheet004.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S024(1).files/sheet005.htm''', 'd:/107_S024(1)_sheet005.htm')

    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S024(2).files/sheet001.htm''', 'd:/107_S024(2)_sheet001.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S024(2).files/sheet002.htm''', 'd:/107_S024(2)_sheet002.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S024(2).files/sheet003.htm''', 'd:/107_S024(2)_sheet003.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S024(2).files/sheet004.htm''', 'd:/107_S024(2)_sheet004.htm')
    write_url_tofile('''http://163.29.251.188/botedata/%E4%BA%A4%E9%80%9A%E6%B5%81%E9%87%8F/107%E5%B9%B4%E5%BA%A6/HTM/S024(2).files/sheet005.htm''', 'd:/107_S024(2)_sheet005.htm')  
