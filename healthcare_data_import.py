import pandas as pd
import mysql.connector

# Load data
df = pd.read_csv("E:\healthcare_analysis_sql+powerBI\healthcare_data.csv")

# Fix date format for MySQL
#df['transaction_date'] = pd.to_datetime(df['transaction_date'], format="%m/%d/%Y").dt.date

# Connect to MySQL
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="xyz",
    database="healthcare_analysis"
)
cursor = conn.cursor()

# Define the SQL query
sql = """
INSERT INTO healthcare_data (
   Name, Age, Gender, Blood_Type, Medical_Condition, Admission_Date, Doctor, Hospital,
     Insurance_Provider, Billing_Amount,Room_Number,Admission_Type, Discharge_Date,
     Medication,Test_Results
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)
"""

# Convert DataFrame to list of tuples
data = [tuple(row) for row in df.itertuples(index=False)]

# Use executemany for bulk insert
cursor.executemany(sql, data)

# Commit and close
conn.commit()
cursor.close()
conn.close()

print("Bulk insert completed successfully!")