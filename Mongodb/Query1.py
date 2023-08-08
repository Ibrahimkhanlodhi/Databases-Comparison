from pymongo import MongoClient
import time

# MongoDB connection settings
mongodb_uri = "mongodb://localhost:27017/"
database_name = "Library"
collection_name = "library_dataset_250k"


# Cypher query equivalent for MongoDB
query_filter = {"ISBN": "978-1-360-54759-6"}
projection = {
    "Book Title": 1,
    "Author": 1,
    "ISBN": 1,
    "Available": 1,
    "Borrower Name": 1,
    "Email": 1,
    "Phone": 1,
    "Borrow Date": 1,
    "Return Date": 1,
    "_id": 0,
}

# Connect to MongoDB
client = MongoClient(mongodb_uri)
db = client[database_name]
collection = db[collection_name]

query_times = []

# Retrieve a book's information from MongoDB and measure the execution time
for _ in range(30):
    start_time = time.time()
    result = collection.find_one(query_filter, projection)
    end_time = time.time()
    execution_time = end_time - start_time
    query_times.append(execution_time)
    print(result)

# Close the MongoDB connection
client.close()

# Write the execution times to a result file
with open('results_query1_mongodb.txt', 'w') as result_file:
    result_file.write(f"Query 1 execution times: {query_times}\n")
