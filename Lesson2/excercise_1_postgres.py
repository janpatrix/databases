import psycopg2
from psycopg2._psycopg import connection 

try:
    connection = psycopg2.connect(user="patrick",
                                  password="Getting started",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="excercise1")
    connection.set_session(autocommit= True)
    print("Database Connection set up!\n")

except psycopg2.Error as error: 
    print("Error: Could not make connection to the Postgres database", error)

try:
    cursor = connection.cursor()
    print("Cursor set up!\n")

except psycopg2.Error as error:
    print("Error: Could not get Cursor", error)

try:
    cursor.execute("create database lesson2_excercise1")
    print("Database created successfully\n")

except psycopg2.Error as error:
    print("Error: Could not create Database", error)

#Set up Music Store
try:
    cursor.execute("CREATE TABLE IF NOT EXISTS MusicStore(\
        transaction_id      int,\
        customer_name       varchar(80),\
        cashier_name        varchar(80),\
        year                int,\
        albums_purchased    text[])")
    print("Table added\n")

except psycopg2.Error as error:
    print("Error: Could not create Table", error)

try:
    cursor.execute("INSERT INTO MusicStore \
        VALUES (%s, %s, %s, %s, %s)",("1", "Amanda", "Sam", 2000, ["Rubber Soul", "Let It Be"]))
    cursor.execute("INSERT INTO MusicStore \
        VALUES (%s, %s, %s, %s, %s)",("2", "Toby", "Sam", 2000, ["My Generation"]))
    cursor.execute("INSERT INTO MusicStore \
        VALUES (%s, %s, %s, %s, %s)",("3", "Max", "Bob", 2018, ["Meet the Beatles", "Help!"]))
    print("Rows added to Table\n")

except psycopg2.Error as error:
    print("Could not add Rows", error)

try: 
    cursor.execute("SELECT * FROM MusicStore;")

except psycopg2.Error as error: 
    print("Error: select *", error)

row = cursor.fetchone()
print("### Music Store ###")
while row:
   print(row)
   row = cursor.fetchone()
print("\n")

#Set up Music Store 2
try: 
    cursor.execute("CREATE TABLE IF NOT EXISTS MusicStore2 (transaction_id int, \
                                                         customer_name varchar, cashier_name varchar, \
                                                         year int, albums_purchased text);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cursor.execute("INSERT INTO MusicStore2 (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, "Amanda", "Sam", 2000, "Rubber Soul"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cursor.execute("INSERT INTO MusicStore2 (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, "Amanda", "Sam", 2000, "Let it Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cursor.execute("INSERT INTO MusicStore2 (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (2, "Toby", "Sam", 2000, "My Generation"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cursor.execute("INSERT INTO MusicStore2 (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3, "Max", "Bob", 2018, "Help!"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cursor.execute("INSERT INTO MusicStore2 (transaction_id, customer_name, cashier_name, year, albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3, "Max", "Bob", 2018, "Meet the Beatles"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cursor.execute("SELECT * FROM MusicStore2;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cursor.fetchone()
print("### Music Store 2###")
while row:
   print(row)
   row = cursor.fetchone()

#Set up 2NF tables transactions and album sold
# We create two new tables transactions and albums sold and insert data into these tables

try: 
    cursor.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id int, \
                                                           customer_name varchar, cashier_name varchar, \
                                                           year int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cursor.execute("CREATE TABLE IF NOT EXISTS albums_sold (album_id int, transaction_id int, \
                                                          album_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cursor.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, "Amanda", "Sam", 2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cursor.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, "Toby", "Sam", 2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cursor.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (3, "Max", "Bob", 2018))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cursor.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (1, 1, "Rubber Soul"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cursor.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (2, 1, "Let it Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cursor.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (3, 2, "My Generation"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cursor.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (4, 3, "Meet the Beatles"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cursor.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (5, 3, "Help!"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

print("\n### Transactions ###\n")
try: 
    cursor.execute("SELECT * FROM transactions;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()

print("\n### Albums_sold ###\n")
try: 
    cursor.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)
row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()
print("\n")

#Do the join of the 2 Tables
print("\n### JOIN the 2 Tables###\n")
try: 
    cursor.execute("SELECT * FROM transactions JOIN albums_sold ON transactions.transaction_id = albums_sold.transaction_id ;")
except psycopg2.Error as error:
    print(error)

row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()
print("\n")

#Set up 3NF tables transactions, album sold and employees
cur = cursor
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS transactions2 (transaction_id int, \
                                                           customer_name varchar, cashier_id int, \
                                                           year int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS employees (employee_id int, \
                                                       employee_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, "Amanda", 1, 2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, "Toby", 1, 2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (3, "Max", 2, 2018))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)", \
                 (1, "Sam"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)", \
                 (2, "Bob"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)    

print("### Transactions2 ###\n")
try: 
    cur.execute("SELECT * FROM transactions2;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\n### Albums_sold ###\n")
try: 
    cur.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\n### Employees ###\n")
try: 
    cur.execute("SELECT * FROM employees;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

#Do the join of the 3 Tables
print("\n### JOIN the 3 Tables ###\n")
try: 
    cursor.execute("SELECT * FROM (transactions2 JOIN albums_sold ON transactions2.transaction_id = albums_sold.transaction_id) \
        JOIN employees ON transactions2.cashier_id=employees.employee_id ;")
except psycopg2.Error as error:
    print(error)

row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()
print("\n")

#Clean up Database
try: 
    cursor.execute("DROP table MusicStore")
    cursor.execute("DROP table MusicStore2")
    cursor.execute("DROP table transactions")
    cursor.execute("DROP table albums_sold")
    cursor.execute("DROP table transactions2")
    cursor.execute("DROP table employees")

except psycopg2.Error as error:
    print(error)

try: 
    cursor.close()
    connection.close()
    print('Database closed')
except psycopg2.Error as error:
    print(error)


