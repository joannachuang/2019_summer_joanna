
class mppool:
    def __init__(self):
        import Lily.ctao.database as cdb
        import Lily.ctao.hostmetadata as chmd

        self.this_host      =   chmd.hostmetadata()
        self.log_database   =   cdb.database(self.this_host.database)

    def map(self, your_function, your_datalist, message = 'mpool'):
        import pandas
        import datetime
        from multiprocessing import Pool

        cpu_code = self.this_host.cpu_code

        mpPool = Pool( self.this_host.cpu_code)

        dict = { 'time_beg' : datetime.datetime.now(), 
                 'type_fun' : type(your_function).__name__, 
                 'type_data': type(your_datalist).__name__ + '_size(' + str(len(your_datalist)) +')_message(' + message + ')',
                 'host_name': self.this_host.hostname,
                 'host_platform': self.this_host.platform,
                 'host_code_number': self.this_host.cpu_code}

        content = mpPool.map(your_function, your_datalist)

        dict['time_end']    = datetime.datetime.now()      
        dict['time_cost']   = (dict['time_end']- dict['time_beg']).seconds     
 
        df = pandas.DataFrame.from_dict( [dict], orient='columns' ) 
        df.to_sql('data_lily_mppool_log', self.log_database.connect, if_exists='append', index =False)
        
        mpPool.close()
        return content
        
    def run(self, your_function, your_data, message = 'run'):
        import pandas
        import datetime
    
        dict = { 'time_beg' : datetime.datetime.now(), 
                 'type_fun' : type(your_function).__name__, 
                 'type_data': type(your_data).__name__ + '_message(' + message + ')',
                 'host_name': self.this_host.hostname,
                 'host_platform': self.this_host.platform,
                 'host_code_number': self.this_host.cpu_code}

        content = your_function(your_data)

        dict['time_end']    = datetime.datetime.now()      
        dict['time_cost']   = (dict['time_end']- dict['time_beg']).seconds     
 
        df = pandas.DataFrame.from_dict( [dict], orient='columns' ) 
        df.to_sql('data_lily_mppool_log', self.log_database.connect, if_exists='append', index =False)

        return content
