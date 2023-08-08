from pymongo import MongoClient
import time

# MongoDB connection settings
mongodb_uri = "mongodb://localhost:27017/"
database_name = "Library"
collection_name = "library_dataset_250k"


query_filter = {}
projection = {
    "_id": 0,
}

# Connect to MongoDB
client = MongoClient(mongodb_uri)
db = client[database_name]
collection = db[collection_name]

query_times = []

# Retrieve book information from MongoDB and measure the execution time
for _ in range(30):
    start_time = time.time()
    result = collection.find_one(query_filter, projection)
    if result:
        print(result)
    end_time = time.time()
    execution_time = end_time - start_time
    query_times.append(execution_time)

# Close the MongoDB connection
client.close()

# Write the execution times to a result file
with open('results_query3_mongodb.txt', 'w') as result_file:
    result_file.write(f"Query 3 execution times: {query_times}\n")
