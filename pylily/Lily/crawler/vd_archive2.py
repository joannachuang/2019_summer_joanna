#!/usr/bin/env python3.5
import os
import re
import gzip
import numpy
import pandas
import datetime
import Lily.ctao.hostmetadata as hmd
import Lily.ctao.database as cdb

class vd_archive:
    def __init__(self):  
        self.ctaohost   = hmd.hostmetadata()

        today = datetime.datetime.today()

        self.database_filename =  self.ctaohost.warehouse + '/ctao_data_crawler_vehicledetect_{0}.sqlite'.format(today.strftime('%Y%m')) 

        self.database   = cdb.database(self.database_filename)

        self.sub_group  = 'data_crawler_vd'

        self.dict_data = {
          'tpec_vddata':
         ['https://tcgbusfs.blob.core.windows.net/blobtisv/GetVDDATA.xml.gz',
          '<ExchangeTime>(.*)</ExchangeTime>',    '%Y/%m/%dT%H:%M:%S'],

          'tpec_vd':
         ['https://tcgbusfs.blob.core.windows.net/blobtisv/GetVD.xml.gz',
          '<vd:ExchangeTime>(.*)</vd:ExchangeTime>',    '%Y/%m/%dT%H:%M:%S'],
        
          'nfbx_1968':
         ['http://tisvcloud.freeway.gov.tw/xml/1min_incident_data_1968.xml',
          'time="([^"]*)"',                             '%Y-%m-%d %H:%M:%S'],

          'nfbx_rlx1':
         ['http://tisvcloud.freeway.gov.tw/roadlevel_value.xml.gz',
          'updatetime="([^"]*)"',                       '%Y/%m/%d %H:%M:%S'],

          'nfbx_rlx5':
         ['http://tisvcloud.freeway.gov.tw/roadlevel_value5.xml.gz',
          'updatetime="([^"]*)"',                       '%Y/%m/%d %H:%M:%S'],

          'nfbx_vdx1':
         ['http://tisvcloud.freeway.gov.tw/vd_value.xml.gz',
          'updatetime="([^"]*)"',                       '%Y/%m/%d %H:%M:%S'],

          'nfbx_vdx5':
         ['http://tisvcloud.freeway.gov.tw/vd_value5.xml.gz',
          'updatetime="([^"]*)"',                       '%Y/%m/%d %H:%M:%S']}


        #all opendata source
        self.list_df = pandas.DataFrame.from_dict(self.dict_data, orient='index', 
             columns=['url', 'exchange_time_repattern', 'exchange_time_datetimepattern'])

        self.list_df['gzip_context']      = numpy.random.bytes(1)
        self.list_df['download_datetime'] = numpy.datetime64(datetime.datetime.now())
        self.list_df['exchange_datetime'] = numpy.datetime64(datetime.datetime.now())

    def urlfile_toBytes(self, url):
        import io
        import gzip
        import requests

        datafile = io.BytesIO()        
        response = requests.get(url , verify=True)
        for chunk in response.iter_content(chunk_size=512 * 1024): 
            if chunk: # filter out keep-alive new chunks
                datafile.write(chunk)

        context = datafile.getvalue()
        try:
            if  url.endswith('.gz') : context = gzip.decompress(context)
        except:
            context = 'except_'
        return context

    def current(self):
        for key in self.list_df.index:
            url      = self.list_df.at[key, 'url']
            xml_re   = self.list_df.at[key, 'exchange_time_repattern']
            day_pa   = self.list_df.at[key, 'exchange_time_datetimepattern']

            self.list_df.at[key, 'gzip_context'     ] = numpy.random.bytes(1)
            self.list_df.at[key, 'download_datetime'] = numpy.datetime64(datetime.datetime.now())
            self.list_df.at[key, 'exchange_datetime'] = numpy.datetime64(datetime.datetime.now())

            bintext     = self.urlfile_toBytes(url)
            context     = str( bintext  )

            if re.findall(xml_re, context):
                str_time        = re.findall(xml_re, context)[0]
                extime          = datetime.datetime.strptime(str_time , day_pa)
                self.list_df.at[key, 'exchange_datetime'] = extime
                self.list_df.at[key, 'gzip_context']      = gzip.compress(bintext)

        return self.list_df

    def to_database(self):
        df = self.list_df.drop( columns=['url', 'exchange_time_repattern', 'exchange_time_datetimepattern'] )

        df.to_sql(self.sub_group, self.database.connect, if_exists='append', index=True, index_label='category')

        self.database.connect.execute(
            '''delete from {0} where length(gzip_context) = 1'''.format(self.sub_group) )
        
        self.database.connect.execute(
            '''delete from {0} where rowid not in (select max (rowid) 
            from {0} group by category, exchange_datetime)'''.format(self.sub_group) )

        self.database.connect.commit()

    def to_database_idv(self):
        df = self.list_df.drop( columns=['url', 'exchange_time_repattern', 'exchange_time_datetimepattern'] )

        for tableind in df.index:
            tablename = str(tableind)

            subdf = df.loc[tableind:tableind]
            print (subdf)

            subdf.to_sql(tablename, self.database.connect, if_exists='append', index=True, index_label='category')
            self.database.connect.execute(
            '''delete from {0} where length(gzip_context) = 1'''.format(tablename) )
        
        self.database.connect.execute(
            '''delete from {0} where rowid not in (select max (rowid) 
            from {0} group by category, exchange_datetime, gzip_context)'''.format(tablename) )

        self.database.connect.commit()

    def check_database(self):
        df  = pandas.read_sql_query ('''select * from {0} '''.format(self.sub_group), self.database.connect, 
                                     index_col=['category', 'exchange_datetime'], 
                                     parse_dates=['exchange_datetime', 'download_datetime'])
        for key in df.index:
            target_dir  = self.ctaohost.factory + '/' + key[1].strftime('''%Y-%m-%d''')
            target_file = key[1].strftime('''{0}_%Y%m%d_%H%M.gz'''.format(key[0]) )

            if  os.path.exists(target_dir) != True:
                os.mkdir(target_dir)

            with open(target_dir + '/' + target_file, 'wb') as xml:
                xml.write (df.at[key,'gzip_context'])


if __name__ == '__console__' or __name__ == '__main__':
    
    vd = vd_archive()
    vd.current()
    vd.to_database()
#    vd.check_database()
    
    
