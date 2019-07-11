import csv
import docx
import pandas
import datetime

from docx import Document

def check_report_datetime(sz_datetime):
    return

def check_time():
    import Lily.ctao.database as cdb
    import Lily.ctao.nsgstring as nstr
    import Lily.ctao.hostmetadata as chmd
    import re
    host  = chmd.hostmetadata()
    db    = cdb.database(host.database)
    #^\d\d\d\d-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01]) (00|[0-9]|1[0-9]|2[0-3]):([0-9]|[0-5][0-9]):([0-9]|[0-5][0-9])$

    patern0 = r'''(0?[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])'''
    patern1 = r'''([0-2][0-9]):([0-5][0-9])'''
    patern2 = r'''^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])|(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])$'''

    df = db.to_dataframe('hln_0206_3')
    df = df.iloc[1:]

    for ind, row in df.iterrows():
        twoday = [ day for day in re.findall(patern0, row[1])]
        twotim = [ tim for tim in re.findall(patern1, row[2])]
   
        if len(twoday) == 0:
            twoday = [('01','01'), ('01','01')]

        if len(twoday) == 1:
            twoday = [twoday[0],twoday[0]]

        if len(twotim) == 0:
            twotim = [('00','00'), ('00','00')]

        if len(twotim) == 1:
            twotim = [twotim[0], twotim[0]]

        date1 = '2018-{0}-{1} {2}:{3}'.format( twoday[0][0],  twoday[0][1], twotim[0][0],  twotim[0][1] )
        date2 = '2018-{0}-{1} {2}:{3}'.format( twoday[1][0],  twoday[1][1], twotim[1][0],  twotim[1][1] )

        df.iloc[ind]['beg'] = datetime.datetime.strptime(date1, '%Y%m%d %H%M')
        df.iloc[ind]['end'] = datetime.datetime.strptime(date2, '%Y%m%d %H%M')


def check_docx(docx_file_name):
    from Lily.ctao.database     import database 
    from Lily.ctao.nsgstring    import alnum
    from Lily.ctao.hostmetadata import hostmetadata
    from Lily.blacksmith.file_feature import get_feature

    host    = hostmetadata()
    db      = database(host.database)
    doc     = Document(docx_file_name)
    feature = get_feature(docx_file_name)

    excelfile = feature['path'] + '/' + feature['name'] + '.xlsx'
    tablename = (feature['name'] + '_{0}')
    writer    = pandas.ExcelWriter( excelfile , engine = 'xlsxwriter')

    counter = 1
    sheetlist = []
    for tab in doc.tables:
        data1=[]
        for row in tab.rows:
            data1.append( [cell.text for cell in row.cells] )

        df = pandas.DataFrame(data1)
        counter = counter + 1
        table_name = tablename.format( str(counter).zfill(3) )
        sheetlist.append(table_name)
        df.to_sql(table_name, db.connect, if_exists='replace')
        df.to_excel(writer, sheet_name=table_name)

    writer.save()
    writer.close()
    return sheetlist

if __name__ == '__main__':
    sheetlist = check_docx('''D:/kiki7.docx''')

    print(sheetlist)
