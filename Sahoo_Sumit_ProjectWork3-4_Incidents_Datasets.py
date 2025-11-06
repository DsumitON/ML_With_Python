
#########################################################################################

# Use shebang line

#!/usr/bin/python3

#Import Library
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import csv
import sqlite3
import os
import time


# ## **Import Incident Response Dataset from github repository**

# GitHub raw CSV file URL
url = "https://raw.githubusercontent.com/DsumitON/PythonProjectDataset/main/IT_Incident_Data.csv"

# Read the CSV file into a DataFrame
data = pd.read_csv(url)

# Display the first few rows
print("\n\n====================\tValidating data to be imported....\t==================== \n\n",data.head())
print(f"\n\n====================\tThe columns are below and total columns count is {data.columns.value_counts().sum()}\t==================== \n\n",data.columns)

#  **Clean Data**

# Replace ? with Unknown
data.replace('?','Unknown' , inplace=True)
print("\n\n====================\tReplacing column value '?' with 'Unknown' value\t==================== \n\n",data.head())

#Drop irrelevant columns
data.drop(columns=['active','rfc','closed_code','vendor','sys_created_at','closed_at','u_priority_confirmation','cmdb_ci','sys_updated_by','sys_updated_at','sys_created_by','problem_id','caused_by','sys_mod_count','notify'], inplace=True)
print("\n\n====================\tDropping irrelevant columns\t==================== \n\n",data.head())
print(f"\n\n====================\tcolumn Size reduced to {data.columns.value_counts().sum()}\t==================== \n\n",data.columns)

#Drop duplicates rows
data.drop_duplicates()
print("\n\n====================\tDropping duplicate rows if any\t==================== \n\n",data.head())

# Export the modified DataFrame to a CSV file
# **!!! Save file**
data.to_csv('IT_Incident_Data_Final.csv', index=False)
print("\n\n====================\t!!! File Downloaded\t==================== \n\n")

#__________________________________________Database Connection__________________________________________

# Verify if database already existed..
print(f"\nchecking if mydatabase.db is already existed.....\n\n")
time.sleep(3)
# Close the connection if it's open
if 'conn' in locals() and conn:
    conn.close()

# Delete the database file
db_path = 'mydatabase.db'
if os.path.exists(db_path):
    os.remove(db_path)
    print(f"Database '{db_path}' deleted successfully.")
else:
    print(f"\n\n====================\tDatabase '{db_path}' does not exist.\t====================\n\n")


# New databse connection
#creating new database with sqlite3
conn = sqlite3.connect('mydatabase.db')
cursor= conn.cursor()
print(f"\n\nDatabase '{db_path}' is created.\n\n")

#__________________________________________IncidentMaster Table Creation__________________________________________

# Create a table 'IncidentMaster'
table_name = 'IncidentMasterTable'
cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (number VARCHAR(20),incident_state VARCHAR(50),reassignment_count INT,reopen_count INT,made_sla BOOLEAN,caller_id VARCHAR(50),opened_by VARCHAR(50),opened_at DATETIME,contact_type VARCHAR(50),location VARCHAR(100),category VARCHAR(100),subcategory VARCHAR(100),u_symptom VARCHAR(100),impact VARCHAR(50),urgency VARCHAR(50),priority VARCHAR(50),assignment_group VARCHAR(100),assigned_to VARCHAR(100),knowledge BOOLEAN,resolved_by VARCHAR(100),resolved_at DATETIME)")
print(f"\n\nTable '{table_name}' is created.\n\n")

# Show column details of table
cursor.execute(f"PRAGMA table_info({table_name})")
columns = cursor.fetchall()
# Print column names
print(f"\n====================\t{table_name}Table Columns:\t==================== \n\n")
for col in columns:
    print(f"{col[1]}  ({col[2]})")   # col[1] is column name, col[2] is data type


# ======== CSV data insert to Table ========
# Insert CSV data into the table
for _, row in data.iterrows():
    cursor.execute(f"INSERT INTO {table_name} (number,incident_state,reassignment_count,reopen_count,made_sla,caller_id,opened_by,opened_at,contact_type,location,category,subcategory,u_symptom,impact,urgency,priority,assignment_group,assigned_to,knowledge,resolved_by,resolved_at) VALUES (?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tuple(row))

conn.commit()  #commit the changes.  

# Show Row details from table
cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
rows = cursor.fetchall()
print(f"\n\n====================\tValidating {table_name} data...\t==================== \n\n")
for x in rows:
    print(x)

# Close the connection
conn.close()
print(f"\n\nDatabase connection is now closed..\n\n")



#__________________________________________SLA Table Creation__________________________________________

print(f"\n\nCreating SLA table....\n\n")
time.sleep(0.5)
print(f"\n\nOpening Database connection..\n\n")
time.sleep(1) 
#Open database Connection
conn = sqlite3.connect('mydatabase.db')
cursor= conn.cursor()
# Create a table 'SLATable'
table_name = 'SLATable'
cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (number VARCHAR(20),incident_state VARCHAR(50),made_sla BOOLEAN,opened_at DATETIME,resolved_at DATETIME)")

# Show column details of table
# Print column names
print(f"\n====================\t{table_name} Columns:\t==================== \n\n")
cursor.execute(f"PRAGMA table_info({table_name})")
columns = cursor.fetchall()
for col in columns:
    print(f"{col[1]}  ({col[2]})")

# ======== Insert few data from IncidentMasterTable to SLATable ========

cursor.execute(f"""
    INSERT INTO {table_name} (number, incident_state, made_sla, opened_at, resolved_at)
    SELECT number, incident_state, made_sla, opened_at, resolved_at
    FROM IncidentMasterTable AS IM
    WHERE NOT EXISTS (
        SELECT 1 FROM {table_name} AS S WHERE S.number = IM.number
    )
""")
conn.commit() #commit the changes.  


# Show Row details from table
cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
rows = cursor.fetchall()
print(f"\n\n====================\tValidating {table_name} data...\t==================== \n\n")
for x in rows:
    print(x)

# Close the connection
conn.close()
print(f"\n\nDatabase connection is now closed..\n\n")



#__________________________________________IncidentResolution Table Creation__________________________________________

print(f"\n\nCreating IncidentResolution table....\n\n")
time.sleep(0.5)
print(f"\n\nOpening Database connection..\n\n")
time.sleep(1) 
#Open database Connection
conn = sqlite3.connect('mydatabase.db')
cursor= conn.cursor()
# Create a table 'IncidentResolutionTable'
table_name = 'IncidentResolutionTable'
cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (number VARCHAR(20),incident_state VARCHAR(50),made_sla BOOLEAN,opened_at DATETIME,location VARCHAR(100),impact VARCHAR(50),urgency VARCHAR(50),priority VARCHAR(50),resolved_at DATETIME)")

# Show column details of table
# Print column names
print(f"\n====================\t{table_name} Columns:\t==================== \n\n")
cursor.execute(f"PRAGMA table_info({table_name})")
columns = cursor.fetchall()
for col in columns:
    print(f"{col[1]}  ({col[2]})")

# ======== Insert few data from IncidentMasterTable to IncidentResolutionTable ========
cursor.execute(f"""
    INSERT INTO {table_name} (number,incident_state,made_sla,opened_at,location,impact,urgency,priority,resolved_at)
    SELECT number,incident_state,made_sla,opened_at,location,impact,urgency,priority,resolved_at
    FROM IncidentMasterTable
""")
conn.commit()#commit the changes.  


# Show Row details from table
cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
rows = cursor.fetchall()
print(f"\n\n====================\tValidating {table_name} data...\t==================== \n\n")
for x in rows:
    print(x)

# Close the connection
conn.close()
print(f"\n\nDatabase connection is now closed..\n\n")




#__________________________________________IncidentPrioirty Table Creation__________________________________________

print(f"\n\nCreating IncidentPrioirty table....\n\n")
time.sleep(0.5)
print(f"\n\nOpening Database connection..\n\n")
time.sleep(1) 
#Open database Connection
conn = sqlite3.connect('mydatabase.db')
cursor= conn.cursor()
 
# Create a table 'IncidentPrioirtyTable'
table_name = 'IncidentPrioirtyTable'
cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (number VARCHAR(20),incident_state VARCHAR(50),impact VARCHAR(50),urgency VARCHAR(50),priority VARCHAR(50),resolved_at DATETIME)")

# Show column details of table
# Print column names
print(f"\n====================\t{table_name} Columns:\t==================== \n\n")
cursor.execute(f"PRAGMA table_info({table_name})")
columns = cursor.fetchall()
for col in columns:
    print(f"{col[1]}  ({col[2]})")

# ======== Insert few data from IncidentMasterTable to IncidentPrioirtyTable ========

cursor.execute(f"""
    INSERT INTO {table_name} (number,incident_state,impact,urgency,priority,resolved_at)
    SELECT number,incident_state,impact,urgency,priority,resolved_at
    FROM IncidentMasterTable
""")
conn.commit() #commit the changes.

# Show Row details from table
cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
rows = cursor.fetchall()
print(f"\n\n====================\tValidating {table_name} data...\t==================== \n\n")
for x in rows:
    print(x)

# Close the connection
conn.close()
print(f"\n\nDatabase connection is now closed..\n\n")




#__________________________________________ IncidentStatus Table Creation__________________________________________

print(f"\n\nCreating IncidentStatus table....\n\n")
time.sleep(0.5)
print(f"\n\nOpening Database connection..\n\n")
time.sleep(1) 
#Open database Connection
conn = sqlite3.connect('mydatabase.db')
cursor= conn.cursor()
 
# Create a table 'IncidentStatusTable'
table_name = 'IncidentStatusTable'
cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (number VARCHAR(20),incident_state VARCHAR(50),resolved_at DATETIME)")

# Show column details of table
# Print column names
print(f"\n====================\t{table_name} Columns:\t==================== \n\n")
cursor.execute(f"PRAGMA table_info({table_name})") 
columns = cursor.fetchall()
for col in columns:
    print(f"{col[1]}  ({col[2]})")

# ======== Insert few data from IncidentMasterTable to IncidentStatusTable ========

cursor.execute(f"""
    INSERT INTO {table_name} (number,incident_state,resolved_at)
    SELECT number,incident_state,resolved_at
    FROM IncidentMasterTable
""")
conn.commit() #commit the changes.

# Show Row details from table
cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
rows = cursor.fetchall()
print(f"\n\n====================\tValidating {table_name} data...\t==================== \n\n")
for x in rows:
    print(x)

# Close the connection
conn.close()
print(f"\n\nDatabase connection is now closed..\n\n")



#__________________________________________ Show tables in current databse__________________________________________
# Show table in current databse
conn = sqlite3.connect('mydatabase.db')
cursor= conn.cursor()
sql_query = """SELECT name FROM sqlite_master  
  WHERE type='table';"""
cursor.execute(sql_query)
print(f"\n\n====================\tTables in mydatabase.db\t==================== \n\n")
print(cursor.fetchall(),"\n")




#__________________________________________ Export Tables to CSV__________________________________________

print(f"\n\n====================\tExport Tables into csv\t====================\n\n")
time.sleep(0.5)
print(f"\nOpening Database connection..\n\n")
time.sleep(1) 
#Open database Connection
conn = sqlite3.connect('mydatabase.db')
cursor= conn.cursor()

# Define the tables to export
table_name = ['IncidentMasterTable','SLATable','IncidentResolutionTable','IncidentPrioirtyTable','IncidentStatusTable']
csv_filename = ['IncidentMasterTable.csv','SLATable.csv','IncidentResolutionTable.csv','IncidentPrioirtyTable.csv','IncidentStatusTable.csv']



# Fetch all data from the table to CSV

for i in range(len(table_name)):
    TN=table_name[i]
    cursor.execute(f"SELECT * FROM {TN}")
    rows = cursor.fetchall()
    # Get column names
    column_names = [description[0] for description in cursor.description]
    # Write to CSV
    with open(csv_filename[i], mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)  # Write header
        writer.writerows(rows)  # Write rows
    print(f"\nTable '{TN}' exported successfully to '{csv_filename[i]}'.\n")
    time.sleep(0.5)

# Close the connection
conn.close()
print(f"\n\nDatabase connection is now closed..\n\n")



##########################################################################################################################
#
#   Visual part of table Data 
#
##########################################################################################################################


#__________________________________________ SLA Table Visualisation__________________________________________

# Read SLATAble data
df=pd.read_csv('SLATable.csv')

# clean the data
# Delete rows where any column contains NaN, 'Unknown', or null values
df.replace('Unknown', pd.NA, inplace=True)  # Replace 'Unknown' with NaN
df.dropna(inplace=True)  # Drop rows with NaN values
df.reset_index(drop=True, inplace=True)  # Reset index after dropping rows

print("\n\n====================\tSLATable Data after removing NA, Unknown and null values\t==================== \n\n",df.head())

# Adding a new column 'resolved_month' to extract the month
df["resolved_month"] = df['resolved_at'].apply(
	lambda x: x.split('-')[1] if '-' in x else 'Unknown'
)

# Define a month mapping dictionary
month_mapping = {
    '01': 'Jan', '1': 'Jan', '02': 'Feb', '2': 'Feb', '03': 'Mar', '3': 'Mar',
    '04': 'Apr', '4': 'Apr', '05': 'May', '5': 'May', '06': 'Jun', '6': 'Jun',
    '07': 'Jul', '7': 'Jul', '08': 'Aug', '8': 'Aug', '09': 'Sep', '9': 'Sep',
    '10': 'Oct', '11': 'Nov', '12': 'Dec'}

# Map numeric months to MMM format
df['resolved_month'] = df['resolved_month'].map(month_mapping)

# new column 'resolved_year' to extract the Year 
df["resolved_year"] = df['resolved_at'].apply(
	lambda x: x.split('-')[0] if '-' in x else 'Unknown'
)



# keep only 2016 Data as less data in 2017
df = df[df['resolved_year'] != '2017']   

print("\n\n====================\tAdded 'resolved_month' and 'resolved_year' column into SLATable DataFrame\t==================== \n\n",df)

# Count incidents per month for each SLA category (0 or 1)
sla_counts = df.groupby(['resolved_month', 'made_sla']).size().reset_index(name='sla_count')

# Ensure months are in correct order
month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
sla_counts['resolved_month'] = pd.Categorical(sla_counts['resolved_month'], categories=month_order, ordered=True)

# Plot the bar chart
plt.figure(1,figsize=(10, 6))
sns.barplot(x='resolved_month', y='sla_count', hue='made_sla', data=sla_counts, palette={0: 'red', 1: 'green'})

# Labels and title
plt.xlabel("Month")
plt.ylabel("SLA Count")
plt.title("SLA Performance by Month")
plt.legend(title="Made SLA", labels=["Not Met (0)", "Met (1)"])
x=plt.legend(title="Made SLA", labels=["Not Met (0)", "Met (1)"])
plt.xticks(rotation=45)  # Rotate month labels if needed
plt.grid(axis="y", linestyle="--", alpha=0.7)

# Show the plot
plt.show()


#__________________________________________ IncidentPrioirty Table Visualisation__________________________________________

#Read CSV file
df = pd.read_csv('IncidentPrioirtyTable.csv')
print("\n\n======================================== \n\n",df)
#CleanData
df.drop_duplicates()
df.dropna(inplace=True)  # Drop rows with NaN values


# Count the number of incidents per IncidentPrioirty
incident_Priority_counts = df['priority'].value_counts()
print("\n\n====================\tIncident counts per priority\t==================== \n\n", incident_Priority_counts)
# Plot the bar chart for incident_Priority_counts
plt.figure(figsize=(10, 6))
incident_Priority_counts.plot(kind='bar', color='skyblue', alpha=0.8)

# Add data labels to the bar chart
for i, value in enumerate(incident_Priority_counts):
    plt.text(i, value + 1, str(value), ha='center', va='bottom', fontsize=10)

# Labels and title
plt.xlabel("Incident Priority")
plt.ylabel("Count")
plt.title("Incident Counts by Priority")
plt.xticks(rotation=45)  # Rotate x-axis labels if needed
plt.grid(axis="y", linestyle="--", alpha=0.7)

# Show the plot
plt.show()

for i, value in enumerate(incident_Priority_counts):
    print(i,value)
print(enumerate(incident_Priority_counts))
#__________________________________________ IncidentStatus Table Visualisation__________________________________________

#Read CSV file
df = pd.read_csv('IncidentStatusTable.csv')
print("\n\n======================================== \n\n",df)
#CleanData
df.drop_duplicates()
df.dropna(inplace=True)  # Drop rows with NaN values


# Count the number of incidents per IncidentStatusTable
incident_state_counts = df['incident_state'].value_counts()
print("\n\n====================\tIncident counts per priority\t==================== \n\n", incident_state_counts)
# Plot the bar chart for incident_state_counts
plt.figure(figsize=(10, 6))
incident_state_counts.plot(kind='bar', color='skyblue', alpha=0.8)

# Add data labels to the bar chart
for i, value in enumerate(incident_state_counts):
    plt.text(i, value + 1, str(value), ha='center', va='bottom', fontsize=10)

# Labels and title
plt.xlabel("Incident Status")
plt.ylabel("Count")
plt.title("Incident Counts by Status")
plt.xticks(rotation=45)  # Rotate x-axis labels if needed
plt.grid(axis="y", linestyle="--", alpha=0.7)

# Show the plot
plt.show()


#__________________________________________ IncidentResolution Table Visualisation__________________________________________

#Read CSV file
df = pd.read_csv('IncidentResolutionTable.csv')
print("\n\n======================================== \n\n",df)
#CleanData
df.drop_duplicates()
df.dropna(inplace=True)  # Drop rows with NaN values


#convert Dates to week number
from datetime import datetime

# Ensure 'resolved_at' is in datetime format
df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')

# Extract the week number
df['resolved_week'] = df['resolved_at'].dt.isocalendar().week

print("\n\n======================================== \n\n", df)



# Count the resolved of incidents 
incident_resolved_counts = df['resolved_week'].value_counts()
# Sort the plot week number wise
incident_resolved_counts = incident_resolved_counts.sort_index()
print("\n\n====================\tIncident counts per week\t==================== \n\n", incident_resolved_counts)
# Plot the bar chart for incident_state_counts
plt.figure(figsize=(10, 6))
incident_resolved_counts.plot(kind='bar', color='skyblue', alpha=0.8)
# Sort the plot week number wise
incident_resolved_counts = incident_resolved_counts.sort_index()

# Add data labels to the bar chart
for i, value in enumerate(incident_resolved_counts):
    plt.text(i, value + 1, str(value), ha='center', va='bottom', fontsize=10)


# make a weekly average line in plot for resolve count
# Calculate the weekly average of resolved incidents
weekly_avg = incident_resolved_counts.mean()

# Plot the weekly average line
plt.axhline(y=weekly_avg, color='red', linestyle='--', label=f'Weekly Avg: {weekly_avg:.2f}')

# Add legend
plt.legend()
# Labels and title
plt.xlabel("Week wise Incident resolved count")
plt.ylabel("Count")
plt.title("Weekly Incident resolution in 2016")
plt.xticks(rotation=45)  # Rotate x-axis labels if needed
plt.grid(axis="y", linestyle="--", alpha=0.7)

# Show the plot
plt.show()

print(f"\n\n\033[1m====================\tEND OF PROJECT WORK\t====================\033[0m\n\n")

