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
    "RETURN b.`Title` AS title, b.`Author` AS author, b.`ISBN` AS isbn, b.`Available` AS available"
)


with driver.session() as session:
    # Execute the Cypher query and measure the execution time
    for _ in range(30):
        start_time = time.time()
        result = session.run(cypher_query)
        results = [record for record in result]
        end_time = time.time()
        execution_time = end_time - start_time
        query_times.append(execution_time)

        # Print or process the results as needed
        for record in results:
            title = record['title']
            author = record['author']
            isbn = record['isbn']
            available = record['available']


# Write the execution times to a result file
with open(f'results_query4_neo4j.txt', 'w') as result_file:
    result_file.write(f"Query 4 execution times: {query_times}\n")

# Close the Neo4j driver
driver.close()
