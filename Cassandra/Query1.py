import csv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

# Connect to your Cassandra instance
cluster = Cluster(['localhost'], auth_provider=PlainTextAuthProvider(
    username='username', password='password'))
session = cluster.connect('library')

dataset_size = 250000

# Load the corresponding CSV file
filename = 'library_dataset4.csv'

# Insert CSV data into Cassandra
with open(filename, 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)

    for row in csv_data:

        if len(row) != 9:
            continue

        title, author, isbn, available, borrower_name, borrower_email, borrower_phone, borrow_date, return_date = row

        query = f"INSERT INTO books (title, author, isbn, available, borrower_name, email, phone, borrow_date, return_date) VALUES ('{title}', '{author}', '{isbn}', {available}, '{borrower_name}', '{borrower_email}', '{borrower_phone}', '{borrow_date}', '{return_date}')"
        session.execute(query)

# Perform the query 30 times and compute the execution time for Cassandra
query_times = []

for _ in range(30):
    key = 'isbn123'
    start_time = time.time()

    # Execute the SELECT query
    query = f"SELECT * FROM books WHERE isbn = '{key}'"
    result = session.execute(query)

    end_time = time.time()
    execution_time = end_time - start_time
    query_times.append(execution_time)

# Write the execution times to a result file
with open(f'results_query1_cassandra_{dataset_size}.txt', 'w') as result_file:
    result_file.write(f"Cassandra Query 1 execution times: {query_times}\n")

# Close the Cassandra connection
session.shutdown()
cluster.shutdown()
