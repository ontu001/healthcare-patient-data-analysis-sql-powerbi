select count(*) from healthcare_data;
truncate table healthcare_data;

-- check local infile active styatus
show variables like 'local_infile';
-- Actice local infile
set global local_infile = 1;

-- import the csv data
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/healthcare_data.csv'
into table healthcare_data
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\r\n' -- for using windows
ignore 1 rows
(Name, Age, Gender, Blood_Type, Medical_Condition, Admission_Date, Doctor, Hospital, Insurance_Provider, 
Billing_Amount,Room_Number,Admission_Type, Discharge_Date,Medication,Test_Results);
 
 
 
 


