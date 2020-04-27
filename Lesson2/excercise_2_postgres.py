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
    cursor.execute("create database lesson2_excercise2")
    print("Database created successfully\n")

except psycopg2.Error as error:
    print("Error: Could not create Database", error)

#Set up tables 

# ALBUMS SOLD
try: 
    cursor.execute("CREATE TABLE IF NOT EXISTS albums_sold (album_id int, transaction_id int, \
                                                          album_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
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

# TRANSACTIONS 2
cur = cursor
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS transactions2 (transaction_id int, \
                                                           customer_name varchar, cashier_id int, \
                                                           year int);")
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

print("\n### Transactions ###\n")
try: 
    cur.execute("SELECT * FROM transactions2;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone() 

# EMPLOYEES
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS employees (employee_id int, \
                                                       employee_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
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
print("\n")

#AMOUNT SPEND
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS amount_spend (transaction_id int, \
                                                       amount_spend int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cursor.execute("INSERT INTO amount_spend (transaction_id, amount_spend) \
                 VALUES (%s, %s)", \
                 (1, 40))
    cursor.execute("INSERT INTO amount_spend (transaction_id, amount_spend) \
                 VALUES (%s, %s)", \
                 (2, 19))
    cursor.execute("INSERT INTO amount_spend (transaction_id, amount_spend) \
                 VALUES (%s, %s)", \
                 (3, 45))

except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)  

print("### Amount Spend ###\n")
try: 
    cur.execute("SELECT * FROM amount_spend;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()
print("\n")

#3-Way JOIN
print("### JOINED Table###")
try: 
    cursor.execute("SELECT * FROM ((transactions2 JOIN albums_sold ON transactions2.transaction_id = albums_sold.transaction_id) \
        JOIN employees ON transactions2.cashier_id=employees.employee_id) \
            JOIN amount_spend ON transactions2.transaction_id=amount_spend.transaction_id;")

except psycopg2.Error as error:
    print(error)    

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()
print("\n")

#Now we are trying to denormalize the data and add the amount spend to the transactions

#TRANSACTIONS 3
cur = cursor
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS transactions3 (transaction_id int, \
                                                           cashier_name varchar, cashier_id int, \
                                                           amount_spend int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("INSERT INTO transactions3 (transaction_id, cashier_name, cashier_id, amount_spend) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, "Sam", 1, 40))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO transactions3 (transaction_id, cashier_name, cashier_id, amount_spend) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, "Sam", 1, 19))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO transactions3 (transaction_id, cashier_name, cashier_id, amount_spend) \
                 VALUES (%s, %s, %s, %s)", \
                 (3, "Bob", 2, 45))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

print("\n### Transactions 3 ###\n")
try: 
    cur.execute("SELECT * FROM transactions3;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone() 
print("\n")
#QUERIES
try:
    cursor.execute("SELECT cashier_name, SUM(amount_spend) FROM transactions3 GROUP BY cashier_name")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)
row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone() 
print("\n")
#Clean up Database
try: 
    cursor.execute("DROP table albums_sold")
    cursor.execute("DROP table transactions2")
    cursor.execute("DROP table employees")
    cursor.execute("DROP table amount_spend")
    cursor.execute("DROP table transactions3")


except psycopg2.Error as error:
    print(error)

try: 
    cursor.close()
    connection.close()
    print('Database closed')
except psycopg2.Error as error:
    print(error)


