import csv
from py2neo import Graph, Node, Relationship
import time

# Connect to your Neo4j instance
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"

graph = Graph(uri, auth=(username, password))

# Load CSV data into Neo4j
filename = 'library_dataset4.csv'


def create_or_update_book_node(row):
    book_node = Node("Book", ISBN=row['ISBN'])
    book_node['Book Title'] = row['Book Title']
    book_node['Author'] = row['Author']
    book_node['Available'] = row['Available']
    book_node['Borrower Name'] = row['Borrower Name']
    book_node['Email'] = row['Email']
    book_node['Phone'] = row['Phone']
    book_node['Borrow Date'] = row['Borrow Date']
    book_node['Return Date'] = row['Return Date']

    existing_book = graph.nodes.match("Book", ISBN=row['ISBN']).first()
    if existing_book:
        existing_book.update(book_node)
        return existing_book
    else:
        graph.create(book_node)
        return book_node


with open(filename, 'r') as file:
    csv_data = csv.DictReader(file)

    # Iterate over the rows and insert the data into Neo4j
    for row in csv_data:
        if len(row) != 9:
            continue
        # Skip rows with an incorrect number of values

        book_node = create_or_update_book_node(row)
