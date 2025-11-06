
# Use shebang line
#!/usr/bin/env python3
# coding: utf-8

#_____________________________________________________________________________Vehicle Manufacturing Company Data Analysis_____________________________________________________________________________
print(f"\n\n\033[1m====================\t\tVehicle Manufacturing Company Data Analysis\t\t====================\033[0m\n\n")
# **--- Import Libraries**
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup as bs
import json
import seaborn as sns
import os
import sqlite3

# # Import Data to JSON format using API
#  **--- Get details from https://vpic.nhtsa.dot.gov/api/**

url='https://vpic.nhtsa.dot.gov/api/vehicles/getallmanufacturers?format=json&page=2'
response=requests.get(url)
data=response.json()
print(f"\n\n====================\tRaw Data from API\t==================== \n\n",data)


# **--- Create a dataframe from JSON with pandas**
df=pd.DataFrame(data['Results'])
df.shape
print(f"\n\n====================\tdataframe from JSON\t==================== \n\n",df)


# **--- organising Manufacturer column MFR_Name column values**
df['Mfr_Name']=df['Mfr_Name']
df['Mfr_Name'].head()
MfrName={}
for i in range(0,len(df['Mfr_Name'])):
    MfrName[i]=df['Mfr_Name'][i].split(",")[0]
MfrName
df['Mfr_Name']=MfrName.values()
print(f"\n\n====================\tManufacturer column restructured\t==================== \n\n",df['Mfr_Name'])


# **--- filling empty entries in Mfr_CommonName columns**
#filling empty entries in Mfr_CommonName columns
df['Mfr_CommonName']=df['Mfr_CommonName'].fillna('Unknown')
print(f"\n\n====================\tfilling empty entries in Mfr_CommonName columns\t==================== \n\n",df['Mfr_Name'])


# **--- Updated Dataframe** 
print(f"\n\n====================\tUpdated Dataframe\t==================== \n\n",df.head())


# **--- Cleaning and reformating VehicleTypes column data** 
# Function to process VehicleTypes column
def process_vehicle_type(entry):
    if isinstance(entry, list) and len(entry) > 0:
        return entry[0].get('Name',None)
    else:
        return 'Unknown'  # Return 'Unknown' if entry is not a list

# Apply transformation
df["VehicleTypes"] = df["VehicleTypes"].apply(process_vehicle_type)

# **--- UNknown Entries in VehicleTypes Column** 
#count of only Unknown values in VehicleTypes column
df['VehicleTypes'].value_counts()['Unknown']
print(f"\n\n====================\tFinal Dataframe\t==================== \n\n",df.head())



#__Export Data to CSV__

#export dataframe to csv
df.to_csv('vehicle_manufacturers.csv',index=False)

#_____________________________________________________________________________Import CSV Data to Database using sqlite3_____________________________________________________________________________

# **--- Create Database and table and import CSV data**  

# Read CSV into a DataFrame
df = pd.read_csv('vehicle_manufacturers.csv')

# Convert empty lists to NaN in 'VehicleTypes' column
#df['VehicleTypes'] = df['VehicleTypes'].apply(lambda x: None if x == '[]' else x)

# Create a connection to SQLite database
db_file = "vehicles.db"  # Database file
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Create table (assuming column names from CSV)
df.to_sql("manufacturers", conn, if_exists="replace", index=False)

# Commit and close connection
conn.commit()
conn.close()

print("Database created successfully! Data imported into 'manufacturers' table.")


# **--- Display manufactureres table data**  
# Reconnect to the database
conn = sqlite3.connect(db_file)

# Query the data
query = "SELECT * FROM manufacturers;"
df_query = pd.read_sql(query, conn)

# Close the connection
conn.close()

# Display results
df_query


# **--- Display Number of table present in vehicle database**  
# Connect to the SQLite database
db_file = "vehicles.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Query to list all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

# Fetch and print table names
tables = cursor.fetchall()
print("Tables in the database:", tables)

# Close connection
conn.commit()
conn.close()


# **--- Droping Table** 
# Connect to the SQLite database
db_file = "vehicles.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Query to list all tables
cursor.execute("DROP TABLE manufacturers;")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

# Fetch and print table names
tables = cursor.fetchall()
print("Tables in the database:", tables)

# Close connection
conn.commit()
conn.close()


# **--- Droping Databse/Databse file**
import sqlite3
import os

db_file = "vehicles.db"

# First, close any open connection (if exists)
try:
    conn = sqlite3.connect(db_file)
    conn.close()
except Exception as e:
    print("Error closing connection:", e)

# Now, delete the database file
try:
    os.remove(db_file)
    print("Database file deleted successfully.")
except PermissionError:
    print("Close all applications using 'vehicles.db' and try again.")

# _____________________________________________________________________________Data Visualisation_____________________________________________________________________________ 

# **--- Bar plot for Number of Vehicle Types available**  

# plot data country with number of manufacture name with vehicle type in legend
df['VehicleTypes'].value_counts().plot(kind='bar')
# Add data labels to the bar chart
for i, value in enumerate(df['VehicleTypes'].value_counts()):
    plt.text(i, value + 1, str(value), ha='center', va='bottom', fontsize=10)
plt.title('Vehicle Types')
plt.xlabel('Vehicle Type')
plt.ylabel('Number of Manufacturers')
plt.xticks(rotation=10)
plt.show()


# **--- plot for Country wise manufacturers**  
sns.countplot(x='Country', data=df)
plt.xlabel('Country')
plt.ylabel('Counts')
# Add data labels to the bar chart
for i, value in enumerate(df['Country'].value_counts()):
    plt.text(i, value + 1, str(value), ha='center', va='bottom', fontsize=10)
plt.title('Number of Vehicles by Country')
plt.xticks(rotation=10)
plt.show()

print(f"\n\n\033[1m====================\tEND OF PROJECT WORK\t====================\033[0m\n\n")




