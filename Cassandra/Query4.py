from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import time

# Connect to your Cassandra instance
cluster = Cluster(['localhost'], auth_provider=PlainTextAuthProvider(
    username='username', password='password'))
session = cluster.connect()

# Create the keyspace and table (if they don't exist)
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS library
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

# Use the 'library' keyspace
session.set_keyspace('library')

session.execute("""
    CREATE TABLE IF NOT EXISTS books (
        isbn text PRIMARY KEY,
        title text,
        author text,
        available boolean,
        borrower_name text,
        email text,
        phone text,
        borrow_date text,
        return_date text
    )
""")

session.execute("""
        CREATE TABLE IF NOT EXISTS books_by_title (
        pattern text,
        isbn text,
        title text,
        author text,
        available boolean,
        borrower_name text,
        email text,
        phone text,
        borrow_date text,
        return_date text,
        PRIMARY KEY (pattern, isbn)
    )
""")

# Define the dataset size
dataset_size = 1000000

# Load the corresponding CSV file
filename = 'library_dataset.csv'

# Insert the data into Cassandra
with open(filename, 'r') as file:
    csv_data = csv.reader(file)

    # Skip header row if present
    next(csv_data)

    # Iterate over the rows and insert the data into Cassandra
    for row in csv_data:

        if len(row) != 9:
            continue

        title, author, isbn, available, borrower_name, borrower_email, borrower_phone, borrow_date, return_date = row

        query_books = f"""
            INSERT INTO books (isbn, title, author, available, borrower_name, email, phone, borrow_date, return_date)
            VALUES ('{isbn}', '{title}', '{author}', {available}, '{borrower_name}', '{borrower_email}', '{borrower_phone}', '{borrow_date}', '{return_date}')
        """
        session.execute(query_books)

        query_books_by_title = f"""
            INSERT INTO books_by_title (title, isbn, author, available, borrower_name, borrower_email, borrower_phone, borrow_date, return_date)
            VALUES ('{title}', '{isbn}', '{author}', {available}, '{borrower_name}', '{borrower_email}', '{borrower_phone}', '{borrow_date}', '{return_date}')
        """
        session.execute(query_books_by_title)

query_times = []

for _ in range(30):
    title = 'Reverse-engineered cohesive Internet solution'
    start_time = time.time()

    # Execute the SELECT query to retrieve all rows with the specified title
    query = f"SELECT * FROM books_by_title WHERE title = '{title}'"
    rows = session.execute(query)

    results = []

    # Process the retrieved rows if needed
    for row in rows:

        result = {
            'ISBN': row.isbn,
            'Title': row.title,
            'Author': row.author,
            'Available': row.available,
            'Borrower Name': row.borrower_name,
            'Email': row.borrower_email,
            'Phone': row.borrower_phone,
            'Borrow Date': row.borrow_date,
            'Return Date': row.return_date,
            'Modified Field': 'Some modification',
            'Additional Field': 'Some additional data',

        }
        results.append(result)

    end_time = time.time()
    execution_time = end_time - start_time
    query_times.append(execution_time)

# Write the execution times to a result file
with open(f'results_query4_{dataset_size}.txt', 'w') as result_file:
    result_file.write(f"Query 4 execution times: {query_times}\n")

# Close the Cassandra connection
session.shutdown()
cluster.shutdown()
