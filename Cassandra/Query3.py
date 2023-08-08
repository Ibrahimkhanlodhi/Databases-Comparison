import csv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

# Connect to your Cassandra instance
cluster = Cluster(['localhost'], auth_provider=PlainTextAuthProvider(
    username='username', password='password'))
session = cluster.connect('library')

dataset_size = 250000

# Create the table books_by_isbn if it doesn't exist
create_table_query = '''
    CREATE TABLE IF NOT EXISTS books_by_isbn (
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
'''
session.execute(create_table_query)

# Load the corresponding CSV file
filename = 'library_dataset4.csv'

# Insert CSV data into Cassandra
with open(filename, 'r') as file:
    csv_data = csv.reader(file)

    # Skip header row if present
    next(csv_data)

    for row in csv_data:

        if len(row) != 9:
            continue
        # Skip rows with an incorrect number of values

        title, author, isbn, available, borrower_name, borrower_email, borrower_phone, borrow_date, return_date = row

        # Insert into Cassandra with the modified data model
        query = f"INSERT INTO books_by_isbn (pattern, isbn, title, author, available, borrower_name, email, phone, borrow_date, return_date) VALUES ('book:', '{isbn}', '{title}', '{author}', {available}, '{borrower_name}', '{borrower_email}', '{borrower_phone}', '{borrow_date}', '{return_date}')"
        session.execute(query)

query_times = []

for _ in range(30):
    pattern = 'book:'
    start_time = time.time()

    # Execute the SELECT query with the composite primary key
    query = f"SELECT * FROM books_by_isbn WHERE pattern = '{pattern}'"
    result = session.execute(query)

    for _ in result:
        pass

    end_time = time.time()
    execution_time = end_time - start_time
    query_times.append(execution_time)

# Write the execution times to a result file
with open(f'results_query3_{dataset_size}.txt', 'w') as result_file:
    result_file.write(f"Query 3 execution times: {query_times}\n")

# Close the Cassandra connection
session.shutdown()
cluster.shutdown()
