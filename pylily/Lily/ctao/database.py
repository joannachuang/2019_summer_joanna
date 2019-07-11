#//# coding=utf-8
import os
import sqlite3
import pandas
from tkinter import * 

class database:

    def __init__(self , database_path):
        
        self.database_path      = database_path
        self.connect            = sqlite3.connect(database_path)  
        self.connect.enable_load_extension(True)

        import Lily.ctao.hostmetadata as ho
        self.platform = ho.hostmetadata().platform[:6]
        
        if self.platform == 'Linux-':
            self.connect.load_extension('libspatialite')
        else:
            self.connect.load_extension('mod_spatialite')

        self.cursor             = self.connect.cursor()
        self.alias_count        = 0

    def rename_table (self, src_table, tar_table):
        self.cursor.execute ( '''ALTER TABLE {0}  RENAME TO {1}'''.format (src_table, tar_table) )
        return 

    def rename_geometry_table(self, oldname, newname):
        pd_gc = self.to_dataframe('geometry_columns')
        self.rename_table( oldname , newname)

        pd_gc.at[ pd_gc['f_table_name'] == oldname.lower(), 'f_table_name'] =  newname.lower()

        self.to_table ('geometry_columns', pd_gc.sort_values(by=['f_table_name']) )
        self.connect.commit()

    def clone_table (self, src_table, tar_table):
        self.to_table(tar_table, self.to_dataframe(src_table))
        return 

    def to_dataframe(self, _tablename):
        return pandas.read_sql_query('''select  * from {} '''.format(_tablename), self.connect)

    def to_table(self, _tablename, _dataframe):
        _dataframe.to_sql(_tablename, self.connect, if_exists='replace', index=False)

    def to_table_with_index(self, _tablename, _dataframe):
        _dataframe.to_sql(_tablename, self.connect, if_exists='replace', index=True)

    def drop_table (self, _table):
        self.cursor.execute ( '''drop TABLE if exists {0}'''.format (_table) )
        return 

    def vacuum (self):
        self.connect.commit()
        self.cursor.execute ( '''vacuum''' )
        return 


    def attach_database (self, database_filename , database_alias  ):
        self.alias_count = self.alias_count + 1 
        comm = '''attach database '{0}' as {1} ''' . format (database_filename,  database_alias ) ;
        self.connect.execute( comm )

    def tables (self):
        return pandas.read_sql_query('''select  name, type from sqlite_master where type = 'table' ''', self.connect, index_col=['name'])

class askopenfilename:
    def __init__(self, nametext, argument_list =[ ['source', 'xlsx'], 
                                                  ['target', 'sqlite'], 
                                                  ['arg1',  None], 
                                                  ['arg2',  None] ] ) :

        
        self.label    = {}
        self.entry    = {}
        self.button   = {}
        self.values   = {}

        self.arg_num        = len(argument_list)         
        self.dirname        = os.getcwd()
        self.mainframe      = Tk()
        self.mainframe.title(nametext)
       
        for vars, ind in zip(argument_list, range(self.arg_num)) :
            arg_name = vars[0]
            arg_type = vars[1]

            self.label[arg_name] = Label(self.mainframe, text = arg_name )
            self.entry[arg_name] = Entry(self.mainframe, width = 50)
            self.label[arg_name].grid ( row = ind, column = 0, stick = W, padx = 2, pady = 2) 
            self.entry[arg_name].grid ( row = ind, column = 1, columnspan = 3, stick = W, padx = 2, pady = 2)
            
            if arg_type is not None: 
                fun = lambda name = arg_name, type= arg_type, ind=ind: self.button_fun (name, type, ind) 
                self.button[arg_name] = Button (self.mainframe, text = arg_type , command =  fun)
                self.button[arg_name].grid (row = ind, column = 4, stick = 'nesw', padx = 2, pady = 2)
            else:
                self.button[arg_name] = None

        self.button_quit        = Button(self.mainframe, text = 'ok', command = self.quit )
        self.button_quit.grid   (row = self.arg_num , column = 4, stick = 'nesw', padx = 2, pady = 2) 

        self.mainframe.mainloop()

    def button_fun(self, argname, argtype, ind):
        from tkinter.filedialog import askopenfilename
        filename = askopenfilename(initialdir = self.dirname,
                       filetypes =( ('choose a File',   '*.{0}'.format(argtype) ),   ("All Files","*.*")),
                       title = 'Choose a file.')

        self.entry[argname].delete(0, END)
        self.entry[argname].insert(0, filename)

    def quit(self):

        for key in self.entry :
            self.values[key] =  self.entry[key].get()
        
        self.mainframe.destroy() 

class asktablename:
    def __init__(self) :

        self.mywin = askopenfilename('choose a sqlite data file', [['database' , 'sqlite']] )
        self.mydb  = database( self.mywin.values['database']   )

        tab_list  = self.mydb.tables().index.values

        self.mainframe = Tk()
        self.mainframe.title( 'user table in ' + self.mywin.values['database'] )
 

        self.arg_num = len(tab_list)
        self.radiobutton = {}

        self.radiovalue = StringVar()

        for arg_name, ind in zip( tab_list, range(self.arg_num)) :
            self.radiobutton[arg_name] = Radiobutton(self.mainframe,  text = arg_name, variable = self.radiovalue, value = arg_name )
            self.radiobutton[arg_name].grid ( row = ind, column = 0, stick = 'w', padx = 2, pady = 2) 

        self.button_quit        = Button(self.mainframe, text = 'pick up a table', bg ='red' , command = self.quit )
        self.button_quit.grid   (row = self.arg_num , column = 0, stick = 'nesw', padx = 2, pady = 2) 

        self.mainframe.mainloop()


    def quit(self):    
        self.tablename = self.radiovalue.get()
        self.databasename = self.mywin.values['database'] 

        self.mainframe.destroy() 

def check_module():
    import Lily.ctao.hostmetadata as chmd
    hobj1= chmd.hostmetadata()
    print ('check moudel Lily.ctao.hostmetadata')
    print (hobj1.callname, hobj1.hostname, hobj1.platform)
    print (hobj1.database, hobj1.warehouse, hobj1.factory)

    dobj2 = database(hobj1.database)
    print (dobj2.tables())
    print ('No news is good news')

    if hobj1.platform[:7] =='Windows': 
        ui = asktablename()
        print (ui.mydb.tables())

if __name__ == '__console__' or __name__ == '__main__' :
    check_module()    

    


