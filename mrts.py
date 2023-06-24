import pandas as pd
import insert_mrts
import yaml
import mysql.connector
from create_mrts import create_mrts_tables

# Create MRTS tables
create_mrts_tables()

# Read the YAML configuration file
with open('mrts.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Create a database connection
connection = mysql.connector.connect(
    host=config['host'],
    user=config['user'],
    password=config['pwd'],
    database=config['db']
)
cursor = connection.cursor()

def preprocess(year):
    # Set the file path to the MRTS Excel file
    file_path = 'mrtssales92-present.xls'

    # Read the Excel file for the specified year, skipping header rows and footer rows
    df = pd.read_excel(file_path, sheet_name=year, skiprows=4, skipfooter=47)

    # Map column names to desired names
    column_mapping = {'Unnamed: 1': 'Kind_of_Business', 'Feb. 2021(p)': 'Feb. 2021'}
    df.rename(columns=column_mapping, inplace=True)

    # Drop 'CY CUM' and 'PY CUM' columns if present
    try:
        df.drop(columns=['CY CUM', 'PY CUM'], inplace=True)
    except KeyError:
        pass

    # Drop any columns containing 'TOTAL'
    total_columns = [col for col in df.columns if 'TOTAL' in col]
    df.drop(columns=total_columns, inplace=True)

    # Melt the DataFrame to transform column headers into a 'period' column and corresponding values
    df_melted = df.melt(id_vars='Kind_of_Business', value_vars=df.columns[1:])

    # Replace special values with appropriate values or 0
    df_melted.replace("(S)", 0, inplace=True)
    df_melted.replace("(NA)", 0, inplace=True)
    df_melted.replace("GAFO(1)", "General Merchandise, Apparel, Home Furnishings, and Other Retail Merchandise", inplace=True)

    # Drop any rows with missing values
    df_melted.dropna(axis=0, inplace=True)

    # Convert the 'value' column to float
    df_melted['value'] = df_melted['value'].astype(float)

    # Sort the DataFrame by 'Kind_of_Business' and 'period'
    df_melted.sort_values(['Kind_of_Business', 'variable'], inplace=True)
    df_melted.reset_index(drop=True, inplace=True)
    
    # Rename the 'variable' column to 'period' and convert it to datetime format
    df_melted.rename(columns={'variable': 'period'}, inplace=True)
    df_melted['period'] = df_melted['period'].astype('datetime64[ns]')

    # Return the preprocessed DataFrame
    return df_melted

def process_mrts():
    # Set display options to show all rows and columns
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    # List of sheet names to process
    sheets = ["2021", "2020", "2019", "2018", "2017", "2016", "2015", "2014", "2013", "2012", "2011", "2010", "2009", "2008",
              "2007", "2006", "2005", "2004", "2003", "2002", "2001", "2000", "1999", "1998", "1997", "1996", "1995", "1994", "1993", "1992"]

    # Process all sheets and concatenate the results to df_final
    df_final = pd.DataFrame()  # Create an empty DataFrame to store the results
    for year in sheets:
        df_temp = preprocess(year)
        df_final = pd.concat([df_final, df_temp])

    # Assign a unique business ID based on 'Kind_of_Business' groups
    grouped_df = df_final.groupby('Kind_of_Business')
    df_final['business_id'] = grouped_df.ngroup() + 1
    df_final.set_index('business_id', inplace=True)

    # Select the necessary columns and export to a CSV file
    csv_file = 'output.csv'
    df_final[['Kind_of_Business', 'period', 'value']].to_csv(csv_file, index=True)

    # Return the path to the CSV file and the number of rows processed
    return csv_file, df_final.shape[0]

# Process MRTS data and get the output file and number of rows processed
csv_file, rows_processed = process_mrts()

# Call the function to insert data from CSV and pass the connection object and csv file
insert_mrts.insert_data(connection, cursor, csv_file)

# Close the database connection
cursor.close()
connection.close()

# Read the processed CSV file
df_final = pd.read_csv(csv_file)

# Extract unique business counts
unique_business = df_final['Kind_of_Business'].drop_duplicates()
unique_business_counts = df_final.groupby('Kind_of_Business').size().reset_index(name='counts')
unique_business_counts['business_id'] = unique_business_counts.index + 1
unique_business_counts = unique_business_counts[['business_id', 'Kind_of_Business', 'counts']]

# Print the unique business counts without index
print(unique_business_counts.to_string(index=False))

# Print the number of rows successfully processed
print(f"Successfully processed {rows_processed} rows.")