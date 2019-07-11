import docx
import pandas as pd

# i am not sure how you are getting your data, but you said it is a
# pandas data frame
df = pd.read_excel('''C:/PythonClass/Class08_20190507/1050206_EarthquakeReport.xlsx''',sheet_name=0)
#df = pd.DataFrame(Data)
print(df)
# open an existing document
doc = docx.Document()

# add a table to the end and create a reference variable
# extra row is so we can add the header row
t = doc.add_table(df.shape[0]+1, df.shape[1])

# add the header rows.
for j in range(df.shape[-1]):
    t.cell(0,j).text = df.iloc[0,j]

# add the rest of the data frame
for i in range(df.shape[0]):
    for j in range(df.shape[-1]):
        t.cell(i+1,j).text = str(df.values[i,j])

# save the doc
doc.save('C:/PythonClass/Class08_20190507/test.docx')
