import csv
import mysql.connector

# MySQL server configuration
mysql_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password',
    'database': 'library'
}


csv_file_path = 'library_dataset4.csv'


def create_connection():
    try:
        conn = mysql.connector.connect(**mysql_config)
        return conn
    except mysql.connector.Error as err:
        print("Error connecting to MySQL:", err)
        return None


def convert_to_integer(value):
    return 1 if value == 'TRUE' else 0


def import_csv_to_mysql(conn, csv_file_path):
    if not conn:
        return

    try:
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Prepare the SQL query for inserting data
            insert_query = "INSERT INTO library_dataset4 (BookTitle, Author, ISBN, Available, BorrowerName, Email, Phone, BorrowDate, ReturnDate) VALUES (%(Book Title)s, %(Author)s, %(ISBN)s, %(Available)s, %(Borrower Name)s, %(Email)s, %(Phone)s, %(Borrow Date)s, %(Return Date)s)"

            cursor = conn.cursor()

            for row in csv_reader:

                row['Available'] = convert_to_integer(row['Available'])

                # Execute the query with the data from the CSV
                cursor.execute(insert_query, row)

            # Commit the changes to the database
            conn.commit()
            print("CSV data imported successfully!")

    except mysql.connector.Error as err:
        print("Error importing CSV data:", err)

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


if __name__ == "__main__":
    connection = create_connection()
    import_csv_to_mysql(connection, csv_file_path)
