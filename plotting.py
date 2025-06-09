
import pymysql
import pandas as pd
import matplotlib.pyplot as plt

host = "localhost"
port = 3306
database = "bigdata_db"
username = "root"
password = ""

conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
cursor = conn.cursor()

# Prepare the SQL query to insert data into the table
sql_query = f"SELECT Formatted_Date, avg_temperature FROM weather_exp"

# Execute the SQL query
cursor.execute(sql_query)

rows = cursor.fetchall()

# Print the results
for row in rows:
    print(row)

columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)

# Convert 'Formatted_Date' to datetime with error handling
try:
    df['Formatted_Date'] = pd.to_datetime(df['Formatted_Date'], errors='coerce')
except pd.errors.DateParseError as e:
    print(f"Error parsing dates: {e}")

# Drop rows with NaN values in 'Formatted_Date'
df = df.dropna(subset=['Formatted_Date'])

# Print the DataFrame
df = df.sort_values(by='Formatted_Date', ascending=True)
print(df)

# Box plot
df.boxplot(column='avg_temperature', vert=False, grid=False)
plt.title('Box Plot')
plt.show()

# Area plot
df.plot.area(y='avg_temperature', title='Area Plot')
plt.show()

# Histogram
df['avg_temperature'].plot(kind='hist')
plt.title('Histogram')
plt.show()

# Commit the changes
conn.commit()
conn.close()


