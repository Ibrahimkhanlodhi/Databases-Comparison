from pymongo import MongoClient
import time

# MongoDB connection settings
mongodb_uri = "mongodb://localhost:27017/"
database_name = "Library"
collection_name = "library_dataset_250k"


query_filter = {}
projection = {
    "_id": 0,
    "Book Title": 1,
    "Author": 1,
    "ISBN": 1,
    "Available": 1
}

# Connect to MongoDB
client = MongoClient(mongodb_uri)
db = client[database_name]
collection = db[collection_name]

query_times = []

# Retrieve book information from MongoDB and measure the execution time
for _ in range(30):
    start_time = time.time()
    result = collection.find(query_filter, projection)
    results = list(result)  # Convert the cursor to a list of dictionaries
    end_time = time.time()
    execution_time = end_time - start_time
    query_times.append(execution_time)


# Close the MongoDB connection
client.close()

# Write the execution times to a result file
with open('results_query4_mongodb.txt', 'w') as result_file:
    result_file.write(f"Query 4 execution times: {query_times}\n")
