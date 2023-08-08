import csv
from py2neo import Graph, Node, Relationship
import time

# Connect to your Neo4j instance
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"

graph = Graph(uri, auth=(username, password))


# Define the Cypher query to retrieve a book's information
cypher_query = (
    "MATCH (b:Book {ISBN: '978-1-5452-4534-7'}) "
    "RETURN b.`Book Title` AS title, b.Author AS author, b.ISBN AS isbn, "
    "b.Available AS available, b.`Borrower Name` AS borrower_name, "
    "b.Email AS email, b.Phone AS phone, b.`Borrow Date` AS borrow_date, "
    "b.`Return Date` AS return_date"
)

query_times = []

# Retrieve a book's information from Neo4j and measure the execution time
for _ in range(30):
    start_time = time.time()
    result = graph.run(cypher_query).evaluate()
    end_time = time.time()
    execution_time = end_time - start_time
    query_times.append(execution_time)
    print(result)

# Write the execution times to a result file
with open('results_query1_neo4j_py2neo.txt', 'w') as result_file:
    result_file.write(f"Query 1 execution times: {query_times}\n")
