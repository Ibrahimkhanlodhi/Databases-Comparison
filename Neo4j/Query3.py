
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


cypher_query = (
    "MATCH (b:Book) "
    "RETURN b"

)


with driver.session() as session:
    # Execute the Cypher query and measure the execution time
    start_time = time.time()
    for _ in range(30):
        result = session.run(cypher_query)
        record = result.single()  # Retrieve a single record from the query result
        if record:
            # Access the Book node (b represents the 'Book' node)
            print(record['b'])
        end_time = time.time()
        execution_time = end_time - start_time
        query_times.append(execution_time)

# Write the execution times to a result file
with open('results_query3_neo4j.txt', 'w') as result_file:
    result_file.write(f"Query 3 execution times: {query_times}\n")

# Close the Neo4j driver
driver.close()
