import csv
import matplotlib.pyplot as plt
import numpy as np


def process_data(data):
    first_values = []
    averages = []

    for sublist in data:
        first_value = float(sublist[0])
        first_values.append(first_value)

        remaining_values = [float(value) for value in sublist[1:]]
        average = sum(remaining_values) / \
            len(remaining_values)
        averages.append(average)

    return first_values, averages


file_mysql = [
    "MySQL/results/250K.csv",
    "MySQL/results/500K.csv",
    "MySQL/results/750K.csv",
    "MySQL/results/1M.csv"
]

allValues_mySql = []

for file in file_mysql:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)

        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)

        second_row = next(csv_reader)

        values = second_row[1:]

        allValues_mySql.append(values)

firstvalue_mySql, average_of_theRest = process_data(allValues_mySql)


allValues_reddis = []
file_reddis = [
    "Redis/results/250k.csv",
    "Redis/results/500k.csv",
    "Redis/results/750k.csv",
    "Redis/results/1M.csv"
]
for file in file_reddis:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)

        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)

        second_row = next(csv_reader)

        values = second_row[1:]

        allValues_reddis.append(values)

firstvalue_redis, average_of_theRest_redis = process_data(allValues_reddis)


allValues_cassandra = []

file_cassandra = [
    "Cassandra/Results/250k.csv",
    "Cassandra/Results/500K.csv",
    "Cassandra/Results/750K.csv",
    "Cassandra/Results/1M.csv"
]
for file in file_cassandra:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)

        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)

        second_row = next(csv_reader)

        values = second_row[1:]

        allValues_cassandra.append(values)

firstvalue_cassandra, average_of_theRest_cassandra = process_data(
    allValues_cassandra)


allValues_mongodb = []

file_mongodb = [
    "Mongodb/results/250k.csv",
    "Mongodb/results/500k.csv",
    "Mongodb/results/750k.csv",
    "Mongodb/results/1M.csv"
]

for file in file_mongodb:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)

        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)

        second_row = next(csv_reader)

        values = second_row[1:]

        allValues_mongodb.append(values)

firstvalue_mongodb, average_of_theRest_mongodb = process_data(
    allValues_mongodb)


allValues_neo4j = []
file_neo4j = [
    "Neo4j/results/250k.csv",
    "Neo4j/results/500k.csv",
    "Neo4j/results/750k.csv",
    "Neo4j/results/1M.csv"
]


for file in file_neo4j:
    with open(file, 'r') as f:
        csv_reader = csv.reader(f)

        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)

        second_row = next(csv_reader)

        values = second_row[1:]

        allValues_neo4j.append(values)

firstvalue_neo4j, average_of_theRest_neo4j = process_data(allValues_neo4j)


dataset1 = firstvalue_neo4j[0]
dataset2 = firstvalue_neo4j[1]
dataset3 = firstvalue_neo4j[2]
dataset4 = firstvalue_neo4j[3]

datasets = ['250k', '500k', '750k', '1m']
response_times = [dataset1, dataset2, dataset3, dataset4]

# Plotting bar plot
plt.figure(figsize=(4, 5))
plt.bar(datasets, response_times, width=0.1)
plt.xlabel('Dataset')
plt.ylabel('Response Time')
plt.title('Response Time for Different Datasets')
plt.grid(True)
plt.show()


databases = ['mySql', 'redis',  'cassandra', 'mongodb',]
dataset_sizes = ["250k", "500k", "750k", "1m"]
response_times = [

    [firstvalue_mySql[0], firstvalue_redis[0],
        firstvalue_cassandra[0], firstvalue_mongodb[0]],
    [firstvalue_mySql[1], firstvalue_redis[1],
        firstvalue_cassandra[1], firstvalue_mongodb[1]],
    [firstvalue_mySql[2], firstvalue_redis[2],
        firstvalue_cassandra[2], firstvalue_mongodb[2]],
    [firstvalue_mySql[3], firstvalue_redis[3],
        firstvalue_cassandra[3], firstvalue_mongodb[3]]

]


# Plotting grouped bar plot
plt.figure(figsize=(10, 6))
bar_width = 0.15
space_between_bars = 0
index = np.arange(len(response_times[0]))

for i, times in enumerate(response_times):
    plt.bar(index + (bar_width + space_between_bars) * i, times,
            bar_width, alpha=0.7, label=f'Dataset {dataset_sizes[i]}')

plt.xlabel('Dataset')
plt.ylabel('Response Time (ms)')
plt.title('Response Time for Different Databases and Datasets')

plt.xticks(index + bar_width * (len(response_times) / 2 - 0.5),
           [f' {databases[i]}' for i in range(len(response_times[0]))])

plt.legend()
plt.grid(True)
plt.show()
