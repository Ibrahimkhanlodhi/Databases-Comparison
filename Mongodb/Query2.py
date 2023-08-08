from pymongo import MongoClient
import time

# MongoDB connection settings
mongodb_uri = "mongodb://localhost:27017/"
database_name = "Library"
collection_name = "library_dataset_250k"

query_filter = {}
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

# Skip and limit values for pagination
start_rank = 0
limit = 10

# Connect to MongoDB
client = MongoClient(mongodb_uri)
db = client[database_name]
collection = db[collection_name]

query_times = []

# Retrieve book information from MongoDB and measure the execution time
for _ in range(30):
    start_time = time.time()
    result = collection.find(query_filter, projection).sort(
        "Available", -1).skip(start_rank).limit(limit)
    for record in result:
        print(record)
    end_time = time.time()
    execution_time = end_time - start_time
    query_times.append(execution_time)

# Close the MongoDB connection
client.close()

# Write the execution times to a result file
with open('results_query2_mongodb.txt', 'w') as result_file:
    result_file.write(f"Query 2 execution times: {query_times}\n")
