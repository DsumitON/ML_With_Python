# Use shebang line on your Python programming assignment 9 (#!/usr/bin/python3 or #!/usr/bin/env python3)


#!/usr/bin/python3 # shebang line


# Print matplotlib version to the screen as shown in screenshot.
import numpy as numpy
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ## **Import 911 calls Dataset from github repository**

# GitHub raw CSV file URL
url = "https://raw.githubusercontent.com/DsumitON/PythonProjectDataset/main/EmergencyCall.csv"

# Read the CSV file into a DataFrame
df = pd.read_csv(url)

# Display the first few rows
print("\n\n====================\tValidating 911 calls data import....\t==================== \n\n",df.head())
print(f"\n\n====================\tThe columns are below and total columns count is {df.columns.value_counts().sum()}\t==================== \n\n",df.columns)

df['Year'] = df['timeStamp'].apply(lambda x: x.split('-')[0])
df['Month']=df['timeStamp'].apply(lambda x: x.split('-')[1])
df['Month']=df['Month'].apply(lambda x:x.replace('01','Jan'))
df['Month']=df['Month'].apply(lambda x:x.replace('02','Feb'))
df['Month']=df['Month'].apply(lambda x:x.replace('03','Mar'))
df['Month']=df['Month'].apply(lambda x:x.replace('04','Apr'))
df['Month']=df['Month'].apply(lambda x:x.replace('05','May'))
df['Month']=df['Month'].apply(lambda x:x.replace('06','Jun'))
df['Month']=df['Month'].apply(lambda x:x.replace('07','Jul'))
df['Month']=df['Month'].apply(lambda x:x.replace('08','Aug'))
df['Month']=df['Month'].apply(lambda x:x.replace('09','Sep'))
df['Month']=df['Month'].apply(lambda x:x.replace('10','Oct'))
df['Month']=df['Month'].apply(lambda x:x.replace('11','Nov'))
df['Month']=df['Month'].apply(lambda x:x.replace('12','Dec'))

df['zip']=str(df['zip'])
df['zip']=df['zip'].fillna('Unknown')
df['zip']=df['zip'].apply(lambda x: str(x).split('.')[0])
print(df)


#Compute - top Zipcodes for 911 
top_zip=df['zip'].value_counts(sort='TRUE').head(10)
print("\n\n====================\tTop 10 Zipcodes used for 911 are....\t==================== \n\n",top_zip)

#Compute - What are the top townships (twp) for 911 calls and Question
top_twp=df['twp'].value_counts(sort='TRUE').head(5)
print("\n\n====================\tTop townships (twp) for 911 calls....\t==================== \n\n",top_twp)

#Compute - the most common reason for 911 calls based on the Reason Column
top_reason=df['title'].value_counts()
top_10reason=df['title'].value_counts(sort='TRUE').head(10)
print("====================\tTop 10 Reasons for 911 calls are :\t==================== \n",top_10reason)

#Compute - Plot barchart using matplot for 911 calls by Reason
#Plot the bars horizontally
top_10reason.plot(kind='bar', figsize=(8, 6), color='skyblue',)
# Add labels and title
plt.xlabel('Count')
plt.ylabel('Reason')
plt.title('Top 10 Reasons for 911 Calls')
plt.xticks(rotation=35,fontsize=6)
# Add data labels to the barh chart
for i, value in enumerate(top_10reason):
    plt.text(i, value + 1, str(value), ha='center', va='bottom', fontsize=10)
plt.show()

#day got maximum calls for EMS- Busyday!!
#Formating date timestamp
df['Day']=df['timeStamp'].apply(lambda x: x.split(' ')[0])
busyday=df['Day'].value_counts()
print(f"\n\n====================\t{busyday.index[0]} got most of the call i.e around {busyday.iloc[0]} numbers\t==================== \n\n",busyday)

#countplot of the Day of Week column with the hue based on the Reason column
date = pd.to_datetime(df['Day'])
# Change date to day (if you intended to extract the day of the week)\n",
df['DayOfWeek'] = date.dt.day_name()
daycounts=df['DayOfWeek'].value_counts()
daycounts.plot(kind='bar',ylabel='countsOfCalls',title='Call counts per WeekDay')

# Create a countplot of the DayOfWeek column with the hue based on the Reason column
sns.countplot(data=df, x='DayOfWeek', hue=df['title'].apply(lambda x: x.split(':')[0]))
# Add labels and title
plt.xlabel('Day of the Week')
plt.ylabel('callCount')
plt.title('Countplot of 911 Calls by Day of the Week and Reason')
plt.legend(title='Reason', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

#countplot month-wise
monthcount=df['Month'].value_counts()
sns.countplot(data=df,x='Month',hue=df['title'].apply(lambda x: x.split(':')[0]))
plt.title('Monthly 911 Calls and Reason')
plt.legend(title='Reason', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()


print(f"\n\n\033[1m====================\tEND OF PROJECT WORK\t====================\033[0m\n\n") # print done message