from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import time
# Create a Cassandra cluster
cluster = Cluster(['localhost'], auth_provider=PlainTextAuthProvider(
    username='username', password='password'))

# Connect to the Cassandra session
session = cluster.connect()

# Create a keyspace if it doesn't exist
session.execute(
    "CREATE KEYSPACE IF NOT EXISTS library WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

# Set the keyspace for the session
session.set_keyspace('library')

# Create a table for books
session.execute(
    """
    CREATE TABLE IF NOT EXISTS books (
        title text,
        author text,
        isbn text,
        available boolean,
        borrower_name text,
        email text,
        phone text,
        borrow_date timestamp,
        return_date timestamp,
        PRIMARY KEY (isbn)
    )
    """
)

# Read the dataset CSV file
filename = 'library_dataset.csv'

with open(filename, 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row if present

    # Iterate over the rows and insert the data into Cassandra
    for row in csv_data:
        title, author, isbn, available, borrower_name, email, phone, borrow_date, return_date = row

        # Convert 'available' to boolean
        available = True if available.lower() == 'true' else False

        session.execute(
            """
            INSERT INTO books (title, author, isbn, available, borrower_name, email, phone, borrow_date, return_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (title, author, isbn, available, borrower_name,
             email, phone, borrow_date, return_date)
        )
# Connect to the Cassandra session
session = cluster.connect('library')

# Retrieve and display one row from the books table
row = session.execute("SELECT * FROM books LIMIT 1").one()

if row:
    print(f"Title: {row.title}")
    print(f"Author: {row.author}")
    print(f"ISBN: {row.isbn}")
    print(f"Available: {row.available}")
    print(f"Borrower Name: {row.borrower_name}")
    print(f"Email: {row.email}")
    print(f"Phone: {row.phone}")
    print(f"Borrow Date: {row.borrow_date}")
    print(f"Return Date: {row.return_date}")
else:
    print("No data found")

# Close the Cassandra session and cluster
session.shutdown()
cluster.shutdown()
