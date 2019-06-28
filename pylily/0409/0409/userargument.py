import os
from tkinter import * 

class tkui:
    def __init__(self, nametext, argument_list =[ ['source', 'dir',   'directory'], 
                                                  ['target', 'sqlite', 'file'    ], 
                                                  ['arg1', None, None            ], 
                                                  ['arg2', None, None            ]  ] ) :

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
            arg_ford = vars[2]

            self.label[arg_name] = Label(self.mainframe, text = arg_name )
            self.entry[arg_name] = Entry(self.mainframe, width = 50)
            self.label[arg_name].grid ( row = ind, column = 0, stick = W, padx = 2, pady = 2) 
            self.entry[arg_name].grid ( row = ind, column = 1, columnspan = 3, stick = W, padx = 2, pady = 2)
            
            if arg_type is not None and arg_ford =='file': 
                fun = lambda name = arg_name, type= arg_type, ind=ind: self.button_fun (name, type, ind) 
                self.button[arg_name] = Button (self.mainframe, text = arg_type , command =  fun)
                self.button[arg_name].grid (row = ind, column = 4, stick = 'nesw', padx = 2, pady = 2)

            elif arg_type is not None and arg_ford =='directory': 
                fun = lambda name = arg_name, ind=ind: self.button_dir (name, ind) 
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

    def button_dir(self, argname, ind):
        from tkinter.filedialog import askdirectory
        pathname = askdirectory()
        self.entry[argname].delete(0, END)
        self.entry[argname].insert(0, pathname)

    def quit(self):

        for key in self.entry :
            self.values[key] =  self.entry[key].get()
        
        self.mainframe.destroy() 

def check_module():
    import Lily.ctao.hostmetadata as chmd

    hobj1= chmd.hostmetadata()
    if hobj1.platform[:7] =='Windows': 
        ui = tkui('check_module')
        for ind in  ui.values:
            print (ind , ui.values[ind] )

if __name__ == '__console__' or __name__ == '__main__' :
    check_module()    