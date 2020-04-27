import psycopg2
from psycopg2._psycopg import connection 

#MUSIC STORE WITH STAR SCHEMA
#Set up Database

try:
    connection = psycopg2.connect(user="patrick",
                                  password="Getting started",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="excercise1")
    connection.set_session(autocommit= True)
    cursor = connection.cursor()
    cursor.execute("create database lesson2_excercise3")
    print("Database set up")
except psycopg2.Error as error: 
    print("Error: ", error)

#FACT Table
try: 
    cursor.execute("CREATE TABLE IF NOT EXISTS fact_table (customer_id int, store_id int, \
                    spent float);")
    cursor.execute("INSERT INTO fact_table (customer_id, store_id, spent) \
                 VALUES (%s, %s, %s)", (1, 1, 20.50))
    cursor.execute("INSERT INTO fact_table (customer_id, store_id, spent) \
                 VALUES (%s, %s, %s)", (2, 1, 35.21))

except psycopg2.Error as error: 
    print("Error: Issue creating table", error)

print("### FACT TABLE ###")
cursor.execute("SELECT * FROM fact_table")
row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()
print("\n")

#ITEMS TABLE
try: 
    cursor.execute("CREATE TABLE IF NOT EXISTS item_table (customer_id int, item_number int, \
                    item_name varchar);")
    cursor.execute("INSERT INTO item_table (customer_id, item_number, item_name) \
                 VALUES (%s, %s, %s)", (1, 1, "Rubber Soul"))
    cursor.execute("INSERT INTO item_table (customer_id, item_number, item_name) \
                 VALUES (%s, %s, %s)", (2, 3, "Let it be"))

except psycopg2.Error as error: 
    print("Error: Issue creating table", error)

print("### ITEM TABLE ###")
cursor.execute("SELECT * FROM item_table")
row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()
print("\n")

#CUSTOMER TABLE
try: 
    cursor.execute("CREATE TABLE IF NOT EXISTS customer_table (customer_id int, customer_name varchar, \
                    rewards boolean);")
    cursor.execute("INSERT INTO customer_table (customer_id, customer_name, rewards) \
                 VALUES (%s, %s, %s)", (1, "Amanda", True))
    cursor.execute("INSERT INTO customer_table (customer_id, customer_name, rewards) \
                 VALUES (%s, %s, %s)", (2, "Tobey", False))

except psycopg2.Error as error: 
    print("Error: Issue creating table", error)

print("### CUSTOMER TABLE ###")
cursor.execute("SELECT * FROM customer_table")
row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()
print("\n")

#STORE TABLE
try: 
    cursor.execute("CREATE TABLE IF NOT EXISTS store_table (store_id int, state varchar);")
    cursor.execute("INSERT INTO store_table (store_id, state) \
                 VALUES (%s, %s)", (1, 'CA'))
    cursor.execute("INSERT INTO store_table (store_id, state) \
                 VALUES (%s, %s)", (2, 'WA'))

except psycopg2.Error as error: 
    print("Error: Issue creating table", error)

print("### STORE TABLE ###")
cursor.execute("SELECT * FROM store_table")
row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()
print("\n")

#Query 1: Find all the customers that spent more than 30 dollars, 
#who are they, which store they bought it from, location of the store, 
#what they bought and if they are a rewards member.

print("### 1ST QUERY ###")
try: 
    cursor.execute("SELECT customer_name, fact_table.store_id, state, item_name, rewards FROM ((fact_table JOIN customer_table ON fact_table.customer_id=customer_table.customer_id)\
        JOIN item_table ON fact_table.customer_id=item_table.customer_id) \
            JOIN store_table ON fact_table.store_id=store_table.store_id WHERE spent>30;")

except psycopg2.Error as error:
    print(error)    

row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()
print("\n")

# Query 2: How much did Customer 2 spend?
print("### 2ND QUERY ###")
try: 
    cursor.execute("SELECT customer_table.customer_id, spent FROM fact_table JOIN customer_table ON fact_table.customer_id=customer_table.customer_id WHERE customer_table.customer_id=2;")

except psycopg2.Error as error:
    print(error)    

row = cursor.fetchone()
while row:
   print(row)
   row = cursor.fetchone()
print("\n")

#Clean up Database
try: 
    cursor.execute("DROP table fact_table")
    cursor.execute("DROP table item_table")
    cursor.execute("DROP table customer_table")
    cursor.execute("DROP table store_table")


    cursor.close()
    connection.close()
    print('Database closed')
except psycopg2.Error as error:
    print(error)
