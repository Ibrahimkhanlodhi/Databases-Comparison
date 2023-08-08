import csv
import redis
import time

# Connect to your Redis instance
r = redis.Redis(host='localhost', port=6379, db=0)

dataset_size = 250000


# Insert CSV data into Redis
filename = 'library_dataset4.csv'

with open(filename, 'r') as file:
    csv_data = csv.reader(file)

    # Skip header row if present
    next(csv_data)

    # Iterate over the rows and insert the data into Redis
    for row in csv_data:

        if len(row) != 9:
            continue
        # Skip rows with incorrect number of values

        title, author, isbn, available, borrower_name, borrower_email, borrower_phone, borrow_date, return_date = row
        r.hset(f'book:{isbn}', 'Book Title', title)
        r.hset(f'book:{isbn}', 'Author', author)
        r.hset(f'book:{isbn}', 'ISBN', isbn)
        r.hset(f'book:{isbn}', 'Available', available)
        r.hset(f'book:{isbn}', 'Borrower Name', borrower_name)
        r.hset(f'book:{isbn}', 'Email', borrower_email)
        r.hset(f'book:{isbn}', 'Phone', borrower_phone)
        r.hset(f'book:{isbn}', 'Borrow Date', borrow_date)
        r.hset(f'book:{isbn}', 'Return Date', return_date)

query_times = []

for _ in range(30):
    key = 'books:availability'
    start_rank = 0
    end_rank = 9
    start_time = time.time()
    results = r.zrange(key, start_rank, end_rank, withscores=True)
    end_time = time.time()
    execution_time = end_time - start_time
    query_times.append(execution_time)

# Write the execution times to a result file
with open(f'results_query2_{dataset_size}.txt', 'w') as result_file:
    result_file.write(f"Query 2 execution times: {query_times}\n")

# Close the Redis connection
r.close()
