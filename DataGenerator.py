import csv
from faker import Faker
import random
from datetime import timedelta

# Set the number of records for the dataset
dataset_size = 250000

# Initialize Faker
fake = Faker()

# Create a list to store the generated data
data = []

# Generate the data
for _ in range(dataset_size):
    # Generate fake book data
    book_title = fake.catch_phrase()
    book_author = fake.name()
    book_isbn = fake.isbn13()
    book_available = random.choice([True, False])

    # Generate fake borrower data
    borrower_name = fake.name()
    borrower_email = fake.email()
    borrower_phone = fake.phone_number()

    # Generate fake borrowing data
    borrow_date = fake.date_time_this_year()
    return_date = borrow_date + timedelta(days=random.randint(7, 30))

    # Add the data to the dataset list
    data.append([book_title, book_author, book_isbn, book_available,
                 borrower_name, borrower_email, borrower_phone,
                 borrow_date, return_date])

# Define the filename
filename = "library_dataset4.csv"

# Save the dataset as a CSV file
with open(filename, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Book Title', 'Author', 'ISBN', 'Available',
                     'Borrower Name', 'Email', 'Phone',
                     'Borrow Date', 'Return Date'])  # Write header
    writer.writerows(data)

print(f"Dataset of size {dataset_size} saved as {filename}.")
