import mysql.connector
import time

# MySQL server configuration
mysql_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password',
    'database': 'library'
}
# Connect to MySQL
conn = mysql.connector.connect(**mysql_config)
cursor = conn.cursor()

# MySQL query
query = """
SELECT `BookTitle`, Author, ISBN, Available
FROM library_dataset4
"""

# Execute the query and measure the execution time
query_times = []
for _ in range(30):
    start_time = time.time()
    cursor.execute(query)
    result = cursor.fetchall()
    end_time = time.time()
    execution_time = end_time - start_time
    query_times.append(execution_time)

# Close the MySQL connection
cursor.close()
conn.close()

# Write the execution times to a result file
with open('results_query4_mysql_250k.txt', 'w') as result_file:
    result_file.write(f"Query 4 execution times: {query_times}\n")
