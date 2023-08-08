import csv
from py2neo import Graph, Node, Relationship
from neo4j import GraphDatabase
import time

# Connect to your Neo4j instance
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"

driver = GraphDatabase.driver(uri, auth=(username, password))

query_times = []

# Define the Cypher query to retrieve a book's information
cypher_query = (
    "MATCH (b:Book) "
    "RETURN b.`Book Title` AS title, b.Author AS author, b.ISBN AS isbn, "
    "b.Available AS available, b.`Borrower Name` AS borrower_name, "
    "b.Email AS email, b.Phone AS phone, b.`Borrow Date` AS borrow_date, "
    "b.`Return Date` AS return_date "
    "ORDER BY b.Available DESC "
    "SKIP $start_rank LIMIT $limit"
)
start_rank = 0
limit = 10
with driver.session() as session:
    # Execute the Cypher query and measure the execution time
    for _ in range(30):
        start_time = time.time()
        result = session.run(cypher_query, start_rank=start_rank, limit=limit)
        for record in result:  # Directly iterate over the Result object
            print(record)  # Access individual records
        end_time = time.time()
        execution_time = end_time - start_time
        query_times.append(execution_time)
# Write the execution times to a result file
with open('results_query2_neo4j.txt', 'w') as result_file:
    result_file.write(f"Query 2 execution times: {query_times}\n")

# Close the Neo4j driver
driver.close()
