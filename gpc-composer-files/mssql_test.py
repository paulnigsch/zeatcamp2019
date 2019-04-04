# https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-2017



import pyodbc
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = 'tcp:powerbi-data-source.database.windows.net'
database = 'powerbi-data-mart'
username = 'pani'
password = 'XXXXXXXXXXXXXXXX'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()



#Sample select query
cursor.execute("SELECT @@version;")
row = cursor.fetchone()
while row:
    print( row[0])
    row = cursor.fetchone()


#Sample insert query

cursor.execute("INSERT test (a,b,c) values (1,2, 'hallo' )")
#cursor.execute("INSERT SalesLT.Product (Name, ProductNumber, StandardCost, ListPrice, SellStartDate) OUTPUT INSERTED.ProductID VALUES ('SQL Server Express New 20', 'SQLEXPRESS New 20', 0, 0, CURRENT_TIMESTAMP )")
row = cursor.fetchone()

while row:
    print( 'Inserted Product key is ' + str(row[0]))
    row = cursor.fetchone()