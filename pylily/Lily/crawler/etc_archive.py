#!/usr/bin/env python3.5
# prototype  step1 usecase ok
# prototype  step2 interface ok
# prototype  step3 remove assumption-engineering 2018-12-07

import re
import os
import datetime
import pandas, numpy

#CREATE TABLE data_crawler_ETC_M03A_pull(
#  date NUM,
#  pull TEXT,
#  cloud_filename TEXT,
#  local_filename TEXT,
#  size INT,
#  mtime NUM,
#  ctime NUM,
#  melt REAL
#)

class etc_archive:

    def __init__(self, sub_group ='M03A'):
        #
        import Lily.ctao.hostmetadata as chmd
        import Lily.ctao.database as cdb
        self.sub_group      = sub_group
        self.hostmetadata   = chmd.hostmetadata()
        self.database       = cdb.database(self.hostmetadata.database)


        self.sub_warehouse  = '{0}/crawler_ETC_{1}'.format(self.hostmetadata.warehouse, self.sub_group)
        self.excel_filename    = '{0}/data_clawler_ETC_{1}_list.xlsx'.format(self.hostmetadata.warehouse , self.sub_group)
        self.sqlite_tablename  = 'data_crawler_ETC_{0}_list'.format(self.sub_group)
        self.sqlite_tablepull  = 'data_crawler_ETC_{0}_pull'.format(self.sub_group)

        #check/create if not exists directory
        if  not os.path.exists(self.sub_warehouse) :
            os.mkdir(self.sub_warehouse)

        #date regular expresstion YYYYMMDD
        date_YYYYMMDD_pattern = '''([12]\d{3}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01]))'''

        self.url = 'http://tisvcloud.freeway.gov.tw/history/TDCS/{0}/'.format(self.sub_group)   #
        self.cloud_archive_pattern = 'href=\"({0}_{1}\.tar\.gz)\"' .format(self.sub_group,  date_YYYYMMDD_pattern)
        self.local_archive_pattern = '({0}_{1}\.tar\.gz)' .format(self.sub_group , date_YYYYMMDD_pattern)
        self.check_archive_list()

    def download_archive(self, keydate):
        import requests 
        cloud_filename = self.archive_list.loc[keydate]['cloud_filename']
        local_filename = self.archive_list.loc[keydate]['local_filename']
        pull = self.archive_list.loc[keydate]['pull']

        year_path = self.sub_warehouse +'/'+ keydate.strftime('%Y')
        file_path = year_path + '/' + cloud_filename
        
        #check/create if not exists directory
        if  not os.path.exists(year_path) :
            os.mkdir(year_path)
        
        if cloud_filename not in [None,  numpy.nan] and ( local_filename in [None,  numpy.nan] or pull == 'enforce') : 
            curl_path = self.url + '/' + cloud_filename
            #download file from cloud                  
            rrd = requests.get( curl_path, stream = True)      
            with open( file_path, 'wb') as output:
                for chunk in rrd.iter_content(chunk_size=1024): 
                    if chunk: 
                        output.write(chunk)
            
            #mark as downloaded
            self.archive_list.loc[keydate]['pull']                 = 'downloaded'
            self.archive_list.loc[keydate]['local_filename']       = file_path
            self.archive_list.loc[keydate]['size']                 = os.path.getsize(file_path)
            self.archive_list.loc[keydate]['ctime']    = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
            self.archive_list.loc[keydate]['mtime']    = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            print (keydate, cloud_filename)

    def download_archive_list(self):
        for keydate in self.archive_list.index:
            cloud_filename = self.archive_list.loc[keydate]['cloud_filename']
            if cloud_filename not in [None,  numpy.nan]:
                self.download_archive(keydate)

    def save_archive_list(self):
        self.archive_list.to_sql( self.sqlite_tablename , self.database.connect, if_exists='replace', index=True)
        writer = pandas.ExcelWriter(self.excel_filename, engine='xlsxwriter',  datetime_format='yyyy-mm-dd hh:mm', date_format='yyyy-mm-dd')
        self.archive_list.to_excel(writer , self.sub_group )
        workbook    = writer.book
        sheet_all   = writer.sheets[ self.sub_group ]
        sheet_all.set_column('A:C', 20)
        sheet_all.set_column('D:D', 60)
        sheet_all.set_column('E:G', 15)
        writer.save()
        return

    def check_archive_list(self):
        import pathlib
        import requests
        #step 1
        days      = pandas.date_range(datetime.datetime.strptime('2014-01-01', '%Y-%m-%d'), datetime.date.today() + datetime.timedelta(days=90), freq='D')
        self.archive_list = pandas.DataFrame( columns =['date', 'pull', 'cloud_filename', 'local_filename', 'size', 'mtime', 'ctime' , 'melt'])
       
        self.archive_list['date'] = days

        self.archive_list = self.archive_list.set_index ('date')

        #step 2 check list from webpage
        html = requests.get(self.url, verify=False).text
        could_list = re.findall(self.cloud_archive_pattern, html)

        for match_rdset in could_list:
            self.archive_list.loc[ datetime.datetime.strptime(match_rdset[1], '%Y%m%d') ]['cloud_filename'] = match_rdset[0]

        #step 3 check list from work-path (disk)     
        for path, dirs, files in os.walk( self.sub_warehouse ):
            for archive in files :
                filename =  path + '/' + archive
                if re.match( self.local_archive_pattern, archive) and os.path.isfile(filename):
                    keydate = datetime.datetime.strptime(archive[5:13], '%Y%m%d')
                    self.archive_list.loc[keydate]['local_filename']     = filename
                    self.archive_list.loc[keydate]['ctime']    = datetime.datetime.fromtimestamp(os.path.getctime(filename))
                    self.archive_list.loc[keydate]['mtime']    = datetime.datetime.fromtimestamp(os.path.getmtime(filename))
                    self.archive_list.loc[keydate]['size']     = os.path.getsize(filename)
                    self.archive_list.loc[keydate]['pull']     = 'do_nothing'

        #step3 read user pull list
        self.pull_list = pandas.DataFrame()

        if self.sqlite_tablepull in self.database.tables().to_dict('index'):
            self.pull_list = pandas.read_sql_query('''select * from {0};'''.format(self.sqlite_tablepull), self.database.connect, index_col=['date'], parse_dates=['date'])
            for keydate in self.pull_list.index:
                self.archive_list.loc[keydate]['pull'] = self.pull_list.loc[keydate]['pull']
                self.archive_list.loc[keydate]['melt'] = self.pull_list.loc[keydate]['melt']
        
            return self.archive_list

           
if __name__ == '__console__' or __name__ == '__main__':
    import sys
    try:
        for sub_group in ['M03A','M04A','M05A','M06A', 'M07A', 'M08A']:
            print(sub_group)
            ca_etc = etc_archive(sub_group)
            ca_etc.save_archive_list()
            ca_etc.download_archive_list()

            print('the end')

    except IOError :
        print ("")
    except ValueError:
        print ("Could not convert data to an integer.")
    except Exception as ex:
        print('')


    except BaseException as ex:

        import sys
        print ('Unexpected error:', sys.exc_info()[0])

    except :
        print('')
