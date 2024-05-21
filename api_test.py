import requests
import json
import pyodbc

# API endpoint URL
url = "https://environment.data.gov.uk/flood-monitoring/id/stations"

# Send GET request to API
response = requests.get(url)

if response.status_code == 200:
    # Parse the JSON data
    data = response.json()
    
    # Define the connection parameters to the MS SQL Server
    SERVER = r'EU-6FBNTT3\SQLEXPRESS'
    DATABASE = 'training'
    USERNAME = 'vs_user'
    PASSWORD = 'vscode_rules'
    
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

    # Connect to the MS SQL Server
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Create the table if it doesn't already exist
    create_table_query = """
        CREATE TABLE training.dbo.FloodMonitoringStations (
                station_id VARCHAR(50),
                station_url_id VARCHAR(255),
                catchment_name VARCHAR(255),
                river_name VARCHAR(255),
                town VARCHAR(255),
                date_opened VARCHAR(12),
                label VARCHAR(255),
                lat FLOAT,
                long FLOAT
                )
    """
    cursor.execute(create_table_query)

    def first_if_list(value):
        # Check if the value is a list
        if isinstance(value, list) and value:
            # Return the first item in the list if the list is not empty
            return value[0]
        else:
            # Return the value itself if it is not a list or if the list is empty
            return value

    # Iterate through the JSON data and insert it into the table
    for item in data['items']:
        station_id = str(item.get('stationReference', '')).replace("'", "$")
        station_url_id = str(item.get('@id', '')).replace("'", "$")
        catchment_name = str(item.get('catchmentName','')).replace("'", "$")
        river_name = str(item.get('riverName', '')).replace("'", "$")
        town = str(item.get('town', '')).replace("'", "$")
        date_opened = first_if_list(item.get('dateOpened', ''))
        label = str(first_if_list(item.get('label', ''))).replace("'", "$")
        lat = first_if_list(item.get('lat',0))
        long = first_if_list(item.get('long',0))

          # Define insert query
        insert_query = f'''
            INSERT INTO FloodMonitoringStations (
                station_id, 
                station_url_id, 
                catchment_name, 
                river_name, 
                town, 
                date_opened, 
                label, 
                lat, 
                long
            ) VALUES ('{station_id}', '{station_url_id}', '{catchment_name}', '{river_name}', '{town}', '{date_opened}', '{label}', {lat}, {long})
        '''

   
    # Execute insert query with values from the JSON item
        cursor.execute(insert_query)

    # Commit the transaction
    conn.commit()
    
    # Close the connection
    cursor.close()
    conn.close()
    
    print("Data successfully written to MS SQL Server.")

else:
    print(f"Failed to fetch data. HTTP Status code: {response.status_code}")



    