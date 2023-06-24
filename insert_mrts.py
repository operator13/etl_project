import csv

def insert_data(connection, cursor, csv_file):
    # Open the CSV file
    with open(csv_file, 'r') as file:
        # Read the CSV file
        csv_data = csv.reader(file)
        next(csv_data)  # Skip the header row if needed

        # Prepare the list to store the values for batch insertion
        values = []

        # Iterate over each row in the CSV and insert into the database
        for row in csv_data:
            # Extract the values from the CSV row
            business_id = int(row[0])
            kind_of_business = row[1]
            period = row[2]
            value = float(row[3])

            # Append the values as a tuple to the list
            values.append((business_id, kind_of_business, period, value))

        # Create the INSERT query with placeholders
        insert_query = "INSERT INTO mrts (business_id, business, period, value) VALUES (%s, %s, %s, %s)"

        # Execute the INSERT query with the values in batches
        cursor.executemany(insert_query, values)

        # Commit the changes to the database
        connection.commit()

        # Return the count of inserted rows
        return len(values)