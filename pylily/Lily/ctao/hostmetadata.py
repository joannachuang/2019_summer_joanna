# coding=utf-8

class hostmetadata:
    def __init__(self):
        import os, socket, platform, datetime
        from os.path import expanduser
        import multiprocessing as mp

        self.today = datetime.datetime.today()
        
        self.db_filename =  'ctao_rdset_lily_calc1_{0}.sqlite'.format(self.today.strftime('%Y%m')) 

        self.home       = expanduser("~")
        self.callname   = __name__
        self.hostname   = socket.gethostname()
        self.platform   = platform.platform()[:10]        
        self.cwd        = os.getcwd()
        
        self.factory    = self.cwd + '/factory'
        self.warehouse  = self.cwd + '/warehouse'

        self.cpu_code   = mp.cpu_count()

        hostlist = {  #kiki7 desktop Intel(R) Core(TM) i7-2600K CPU @3.40GHz 3.40GHz (code=4/m-threading=8)
                      
                      ('CT-i7-2600K', 'Windows-10') : 
                      (
                            'l:/kiki7_factory',
                            'l:/kiki7_warehouse',
                            13),

                      ('CT-HP400', 'Windows-10') : #hp400 notebook pc 
                      (
                            'd:/hp400_factory',
                            'd:/hp400_warehouse',
                            3),
                      
                      ('Artemis3.hodala', 'Linux-3.10' ) : #Artemis3 vritual machine 
                      (
                            '/mnt/summer/Artemis3_factory',
                            '/mnt/summer/Artemis3_warehouse',
                            32),
                      
                      ('r7920', 'Linux-3.10' ) :           #Dell 7920 
                      (
                            self.home + '/factory',
                            self.home + '/warehouse',
                            23)
            }

        if (self.hostname, self.platform ) in hostlist:
            arglist = hostlist[(self.hostname, self.platform )]
            self.factory    = arglist[0]
            self.warehouse  = arglist[1]
            self.cpu_code   = arglist[2]
        else:
            print (self.hostname, self.platform, self.callname)

        self.database   = self.factory + '/'+ self.db_filename 

        #check/create if not exists directory
        if  not os.path.exists(self.warehouse) :
            os.mkdir(self.warehouse)

        if  not os.path.exists(self.factory) :
            os.mkdir(self.factory)

        return

    def o_redirect(self, output_text = 'ctao_host'):

        import sys, uuid
        self.output_text = self.factory + '/' + output_text + str(uuid.uuid1()) + '.txt'
        
        fout   =   open (self.output_text , 'w+')
        sys.stderr = fout
        
        return

 
if __name__ == '__console__' or __name__ == '__main__':
    hm = hostmetadata()


    
