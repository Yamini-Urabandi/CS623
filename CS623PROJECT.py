import psycopg2
from tabulate import tabulate

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="ap",
    user="postgres",
    password="12345678"
)
#For isolation: SERIALIZABLE
conn.set_isolation_level(3)#follow the serializable 
#For atomicity
conn.autocommit = False

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# Create the Product table
cur.execute("""
    CREATE TABLE Product (
        prodid VARCHAR(10) PRIMARY KEY,
        pname VARCHAR(10),
        price NUMERIC(10, 2)
    )
""")

# Create the Depot table
cur.execute("""
    CREATE TABLE Depot (
        depid VARCHAR(10) PRIMARY KEY,
        addr VARCHAR(10),
        volume NUMERIC(10)
    )
""")

# Create the Stock table with foreign keys and on delete cascade
cur.execute("""
    CREATE TABLE Stock (
        pid VARCHAR(10) REFERENCES Product(prodid) ON DELETE CASCADE ON UPDATE CASCADE,
        dpid VARCHAR(10) REFERENCES Depot(depid) ON DELETE CASCADE ON UPDATE CASCADE,
        quantity VARCHAR(100),
        PRIMARY KEY (pid, did)
    )
""")
# Insert values into the Product table
cur.execute("INSERT INTO Product (prodid, pname, price) VALUES ('P1', 'tape', 2.5)")
cur.execute("INSERT INTO Product (prodid, pname, price) VALUES ('P2', 'tv', 250)")
cur.execute("INSERT INTO Product (prodid, pname, price) VALUES ('P3', 'ver', 80)")

# Insert values into the Depot table
cur.execute("INSERT INTO Depot (depid, addr, volume) VALUES ('D1', 'newyork', 9000)")
cur.execute("INSERT INTO Depot (depid, addr, volume) VALUES ('D2', 'syracuse', 6000)")
cur.execute("INSERT INTO Depot (depid, addr, volume) VALUES ('D4', 'newyork', 2000)")

# Insert values into the Stock table
cur.execute("INSERT INTO Stock (pid, did, quantity) VALUES ('P1', 'D1', '1000')")
cur.execute("INSERT INTO Stock (pid, did, quantity) VALUES ('P1', 'D2', '-100')")
cur.execute("INSERT INTO Stock (pid, did, quantity) VALUES ('P1', 'D4', '1200')")
cur.execute("INSERT INTO Stock (pid, did, quantity) VALUES ('P3', 'D1', '3000')")
cur.execute("INSERT INTO Stock (pid, did, quantity) VALUES ('P3', 'D4', '2000')")
cur.execute("INSERT INTO Stock (pid, did, quantity) VALUES ('P2', 'D4', '1500')")
cur.execute("INSERT INTO Stock (pid, did, quantity) VALUES ('P2', 'D1', '-400')")
cur.execute("INSERT INTO Stock (pid, did, quantity) VALUES ('P2', 'D2', '2000')")

try:
    # Transaction 1: Add a product (p100, cd, 5) in Product and (p100, d2, 50) in Stock
    cur.execute("INSERT INTO Product (prodid, pname, price) VALUES ('P100', 'CD', 5)")
    cur.execute("INSERT INTO Stock (pid, did, quantity) VALUES ('P100', 'D2', 50)")

    # Transaction 2: Change the name of product p1 to pp1 in Product and Stock
    cur.execute("UPDATE Product SET pname = 'PP1' WHERE prodid = 'P1'")

    
    

    # Transaction 3: Delete product p1 from Product and Stock
    
    cur.execute("DELETE FROM Product WHERE prodid = 'P1'")

    # Commit the changes to the database
    conn.commit()

       # Display the contents of the Product table
    cur.execute("SELECT * FROM Product")
    product_data = cur.fetchall()
    headers = ['prodid', 'pname', 'price']
    print("Product Table:")
    print(tabulate(product_data, headers, tablefmt="grid"))

    # Display the contents of the Stock table
    cur.execute("SELECT * FROM Stock")
    stock_data = cur.fetchall()
    headers = ['pid', 'did', 'quantity']
    print("Stock Table:")
    print(tabulate(stock_data, headers, tablefmt="grid"))

except (Exception, psycopg2.DatabaseError) as err:#atom
    print(err)
    print("Transactions could not be completed, so the database will be rolled back before the start of transactions")
    conn.rollback()

finally:
    # Close the cursor and connection
    cur.close()#dur
    conn.close()#dur
    print("PostgreSQL connection is now closed")
