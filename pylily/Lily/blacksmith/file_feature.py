#!/usr/bin/env python3.5
#local mark2



def get_feature(filename):
    import os, datetime

    f_dict = {}

    f_dict['name'], f_dict['extension'] = os.path.splitext(filename)
    f_dict['path'], f_dict['name']      = os.path.split( f_dict['name'] )

    statinfo     = os.stat(filename)
    f_dict['size']  = statinfo.st_size
    f_dict['time_a']    = datetime.datetime.utcfromtimestamp(int( statinfo.st_atime))
    f_dict['time_m']    = datetime.datetime.utcfromtimestamp(int( statinfo.st_mtime))
    f_dict['time_c']    = datetime.datetime.utcfromtimestamp(int( statinfo.st_ctime))

    return f_dict

def get_md5sum(filename):
    import hashlib
    fileobj      = open(filename, 'rb')    
    content      = fileobj.read()
    md5sum = hashlib.md5( content ).hexdigest()    
    fileobj.close()
    print (filename)
    return md5sum

def get_feature_with_md5sum(filename):
    f_dict = get_feature(filename)
    f_dict['md5_sum'] = get_md5sum(filename)   
    return f_dict

def get_all_filename(target_directory):
    import os, pandas

    rdset_rows = []    
    for path, dirs, files in os.walk(target_directory):
        for fname in files:
            src_filename = os.path.join(path, fname)
            rdset_rows.append(  {'file_name' : src_filename })
    df = pandas.DataFrame.from_dict(rdset_rows, orient='columns') 
    return df


def get_all_filefeature_with_md5sum(target_directory):
    import pandas
    from Lily.blacksmith.mppool import mppool
    
    pool = mppool()
    
    file_list = get_all_filename(target_directory)['file_name'].tolist()

    md5 = pool.map(get_feature_with_md5sum, file_list)
    df = pandas.DataFrame.from_dict(md5, orient='columns')

    return  df

def to_database( target_dir ):
    import Lily.ctao.database as cdb
    import Lily.ctao.nsgstring as nstr
    import Lily.ctao.hostmetadata as chmd

    host  = chmd.hostmetadata()
    p1 = nstr.alnum(host.platform)
    h1 = nstr.alnum(host.hostname)
    d1 = nstr.alnum(target_dir)

    db    = cdb.database(host.database)

    dflist = get_all_filefeature_with_md5sum(target_dir)
    table_name = '''data_rdset_filemd5_{0}_{1}_hhot_{2}'''.format(p1, h1, d1) 

    dflist.to_sql(table_name, db.connect, if_exists='replace', index=False)

## TODO 執行這個目錄
## 執行指定目錄 指定database
## 可獨立執行

def check_moudle():
    import sys
    import Lily.ctao.hostmetadata as chmd
    from Lily.blacksmith.mppool import mppool
    pool = mppool()
 
    this_host= chmd.hostmetadata()

    if this_host.platform[:7] =='Windows': 
        from Lily.ctao.userargument import tkui
        ui = tkui('select_target_directory',[['target','sel', 'directory']])
        pool.run(to_database, ui.values['target'], 'get_all_file_feature')
    elif sys.argv == 2:
        pool.run(to_database, sys.argv[1], 'get_all_file_feature')
    else:
        target = input("Enter a directory name:(path)")
        pool.run(to_database, target, 'get_all_file_feature')

if __name__ == '__main__':
    check_moudle()