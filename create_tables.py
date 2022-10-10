import pymysql


# MySQL DB Credentials
username = ''# your MySQL username
password = ''# your MySQL password
host = ""
port=''

# Create database Person from SQL script file
with open('scripts_db/create_db_Person.sql', 'r') as query:
   query = query.read()

conn = pymysql.connect(host=host, user=username, passwd=password)
cur = conn.cursor()
cur.execute(query)
conn.commit()



# Create database Production from SQL script file
with open('scripts_db/create_db_Production.sql', 'r') as query:
   query = query.read()

conn = pymysql.connect(host=host, user=username, passwd=password)
cur = conn.cursor()
cur.execute(query)
conn.commit()



# Create database Sales from SQL script file
with open('scripts_db/create_db_Sales.sql', 'r') as query:
   query = query.read()

conn = pymysql.connect(host=host, user=username, passwd=password)
cur = conn.cursor()
cur.execute(query)
conn.commit()



# Create table Product from SQL script file
with open('scripts_db/create_table_Product.sql', 'r') as query:
   query = query.read()

conn = pymysql.connect(host=host, user=username, passwd=password)
cur = conn.cursor()
cur.execute(query)
conn.commit()



# Create table Person from SQL script file
with open('scripts_db/create_table_Person.sql', 'r') as query:
   query = query.read()

conn = pymysql.connect(host=host, user=username, passwd=password)
cur = conn.cursor()
cur.execute(query)
conn.commit()


# Create table Customer from SQL script file
with open('scripts_db/create_table_Customer.sql', 'r') as query:
   query = query.read()

conn = pymysql.connect(host=host, user=username, passwd=password)
cur = conn.cursor()
cur.execute(query)
conn.commit()


# Create table SalesOrderHeader from SQL script file
with open('scripts_db/create_table_SalesOrderHeader.sql', 'r') as query:
   query = query.read()

conn = pymysql.connect(host=host, user=username, passwd=password)
cur = conn.cursor()
cur.execute(query)
conn.commit()


# Create table SpecialOfferProduct from SQL script file
with open('scripts_db/create_table_SpecialOfferProduct.sql', 'r') as query:
   query = query.read()

conn = pymysql.connect(host=host, user=username, passwd=password)
cur = conn.cursor()
cur.execute(query)
conn.commit()



# Create table SalesOrderDetail from SQL script file
with open('scripts_db/create_table_Sales.SalesOrderDetail.sql', 'r') as query:
   query = query.read()

conn = pymysql.connect(host=host, user=username, passwd=password)
cur = conn.cursor()
cur.execute(query)
conn.commit()
















