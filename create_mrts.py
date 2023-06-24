import mysql.connector
import yaml

def create_mrts_tables():
    # Read the YAML configuration file
    with open('/Users/oantazo/Downloads/MIT_Solutions/week8/MRTS/MRTS/final_project/mrts.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)

    # Create a database connection without specifying the database name
    connection = mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['pwd']
    )
    cursor = connection.cursor()

    # Create the database if it does not exist
    cursor.execute("CREATE DATABASE IF NOT EXISTS mrts")

    # Switch to the newly created database
    cursor.execute("USE mrts")

    # Execute the SQL script to create the tables
    sql_script = '''
    DROP TABLE IF EXISTS unique_business_counts;
    CREATE TABLE unique_business_counts (
      mrts_index INT UNSIGNED NOT NULL AUTO_INCREMENT,
      Kind_of_Business VARCHAR(100) NOT NULL,
      counts INT NOT NULL,
      PRIMARY KEY (mrts_index)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

    DROP TABLE IF EXISTS mrts;
    CREATE TABLE mrts (
      mrts_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
      business_id INT UNSIGNED,
      business VARCHAR(100) NOT NULL,
      period DATETIME,
      value FLOAT,
      PRIMARY KEY (mrts_id),
      UNIQUE (mrts_id),
      KEY idx_fk_business_id (business_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''

    for statement in sql_script.split(';'):
        if statement.strip():
            cursor.execute(statement)

    # Commit the changes and close the database connection
    connection.commit()
    cursor.close()
    connection.close()
    
# Call the function to create the MRTS tables
create_mrts_tables()