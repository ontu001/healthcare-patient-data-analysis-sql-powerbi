CREATE TABLE  healthcare_data (
    "Name" VARCHAR(255),
    "Age" INTEGER,
    "Gender" VARCHAR(10),
    "Blood_Type" VARCHAR(3),
    "Medical_Condition" VARCHAR(255),
    "Admission_date" VARCHAR(255),
    "Doctor" VARCHAR(255),
    "Hospital" VARCHAR(255),
    "Insurance_Provider" VARCHAR(255),
    "Billing_Amount" NUMERIC(12, 2),
    "Room_Number" INTEGER,
    "Admission_Type" VARCHAR(50),
    "Discharge_Date" VARCHAR(255),
    "Medication" VARCHAR(255),
    "Test_Results" VARCHAR(255)
);


select * from healthcare_data;

-- update admission date values to date
UPDATE healthcare_data
SET "Admission_date" = TO_DATE("Admission_date", 'MM/DD/YYYY');

-- update admission date Data type to date
ALTER TABLE healthcare_data
ALTER COLUMN "Admission_date" TYPE DATE
USING "Admission_date"::DATE;


-- update Discharge date values to date
UPDATE healthcare_data
SET "Discharge_Date" = TO_DATE("Discharge_Date", 'MM/DD/YYYY');


-- update admission date Data type to date
ALTER TABLE healthcare_data
ALTER COLUMN "Discharge_Date" TYPE DATE
USING "Discharge_Date"::DATE;


-- Create fact table & dimension tables

-- Create dimension tables

CREATE OR REPLACE PROCEDURE CreateDimensionTables()
LANGUAGE plpgsql    
AS $$ 
BEGIN
    -- Drop and create Dim_Patient
    DROP TABLE IF EXISTS Dim_Patient;
    CREATE TABLE Dim_Patient (
        Patient_ID SERIAL PRIMARY KEY,
        Name VARCHAR(100) NOT NULL,
        Age INT NOT NULL,
        Gender VARCHAR(10) NOT NULL,
        Blood_Type VARCHAR(3) NOT NULL,
        Medical_Condition VARCHAR(100) NOT NULL
    );

    -- Insert distinct patient data (with quoted column names)
    INSERT INTO Dim_Patient (Name, Age, Gender, Blood_Type, Medical_Condition)
    SELECT DISTINCT "Name", "Age", "Gender", "Blood_Type", "Medical_Condition" 
    FROM healthcare_data;

    -- Drop and create Dim_Doctor
    DROP TABLE IF EXISTS Dim_Doctor;
    CREATE TABLE Dim_Doctor (
        Doctor_ID SERIAL PRIMARY KEY,
        Doctor_Name VARCHAR(100) NOT NULL,
        Hospital VARCHAR(100) NOT NULL
    );

    INSERT INTO Dim_Doctor (Doctor_Name, Hospital)
    SELECT DISTINCT "Doctor", "Hospital" 
    FROM healthcare_data;

    -- Drop and create Dim_Hospital
    DROP TABLE IF EXISTS Dim_Hospital;
    CREATE TABLE Dim_Hospital (
        Hospital_ID SERIAL PRIMARY KEY,
        Hospital_Name VARCHAR(100) NOT NULL,
        Insurance_Provider VARCHAR(100) NOT NULL
    );

    INSERT INTO Dim_Hospital (Hospital_Name, Insurance_Provider)
    SELECT DISTINCT "Hospital", "Insurance_Provider" 
    FROM healthcare_data;

    -- Drop and create Dim_Admission
    DROP TABLE IF EXISTS Dim_Admission;
    CREATE TABLE Dim_Admission (
        Admission_ID SERIAL PRIMARY KEY,
        Admission_Type VARCHAR(50) NOT NULL,
        Room_Number INT NOT NULL
    );

    INSERT INTO Dim_Admission (Admission_Type, Room_Number)
    SELECT DISTINCT "Admission_Type", "Room_Number" 
    FROM healthcare_data;

    -- Drop and create Dim_Medication
    DROP TABLE IF EXISTS Dim_Medication;
    CREATE TABLE Dim_Medication (
        Medication_ID SERIAL PRIMARY KEY,
        Medication_Name VARCHAR(100) NOT NULL
    );

    INSERT INTO Dim_Medication (Medication_Name)
    SELECT DISTINCT "Medication" 
    FROM healthcare_data;

    -- Drop and create Dim_Test_Results
    DROP TABLE IF EXISTS Dim_Test_Results;
    CREATE TABLE Dim_Test_Results (
        Test_Result_ID SERIAL PRIMARY KEY,
        Test_Result VARCHAR(100) NOT NULL
    );

    INSERT INTO Dim_Test_Results (Test_Result)
    SELECT DISTINCT "Test_Results" 
    FROM healthcare_data;

END $$;




CALL CreateDimensionTables();

select * from dim_patient;



-- Create a Fact Table
CREATE OR REPLACE PROCEDURE CreateFactTable()
LANGUAGE plpgsql    
AS $$ 
BEGIN
    -- Drop the fact table if it exists
    DROP TABLE IF EXISTS Fact_Admissions;

    -- Create fact table
    CREATE TABLE Fact_Admissions (
        Fact_ID SERIAL PRIMARY KEY,
        Patient_ID INT,
        Doctor_ID INT,
        Hospital_ID INT,
        Admission_ID INT,
        Medication_ID INT,
        Test_Result_ID INT,
        Admission_Date DATE NOT NULL,
        Discharge_Date DATE NOT NULL,
        Billing_Amount DECIMAL(10, 2) NOT NULL
    );

    -- Insert data into the fact table
    WITH DeduplicatedData AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    "Name", "Age", "Gender", "Blood_Type", "Medical_Condition", 
                    "Admission_date", "Doctor", "Hospital", "Insurance_Provider", 
                    "Billing_Amount", "Room_Number", "Admission_Type", "Discharge_Date", 
                    "Medication", "Test_Results"
                ORDER BY 
                    "Admission_date"
            ) AS rn
        FROM healthcare_data
    ),
    FilteredData AS (
        SELECT *
        FROM DeduplicatedData 
        WHERE rn = 1
    )
    INSERT INTO Fact_Admissions (
        Patient_ID, Doctor_ID, Hospital_ID, Admission_ID, Medication_ID, Test_Result_ID, Admission_Date, Discharge_Date, Billing_Amount
    )
    SELECT 
        p.Patient_ID,
        d.Doctor_ID, 
        h.Hospital_ID, 
        a.Admission_ID, 
        m.Medication_ID, 
        t.Test_Result_ID, 
        fd."Admission_date", 
        fd."Discharge_Date",
        fd."Billing_Amount"
    FROM FilteredData AS fd
    JOIN Dim_Patient p 
        ON fd."Name" = p.Name AND fd."Age" = p.Age AND fd."Gender" = p.Gender 
        AND fd."Blood_Type" = p.Blood_Type AND fd."Medical_Condition" = p.Medical_Condition
    JOIN Dim_Doctor d 
        ON fd."Doctor" = d.Doctor_Name AND fd."Hospital" = d.Hospital
    JOIN Dim_Hospital h 
        ON fd."Hospital" = h.Hospital_Name AND fd."Insurance_Provider" = h.Insurance_Provider
    JOIN Dim_Admission a 
        ON fd."Admission_Type" = a.Admission_Type AND fd."Room_Number" = a.Room_Number
    JOIN Dim_Medication m 
        ON fd."Medication" = m.Medication_Name
    JOIN Dim_Test_Results t 
        ON fd."Test_Results" = t.Test_Result;

END $$;


CALL CreateFactTable();

select count(*) from Fact_Admissions;

-- Cheak fact table is okay or not
SELECT Patient_ID, 
       Doctor_ID, 
       Hospital_ID, 
       Admission_ID, 
       Medication_ID, 
       Test_Result_ID, 
       Admission_Date, 
       Discharge_Date, 
       Billing_Amount ,
       COUNT(*) AS duplicate_count
FROM Fact_Admissions
GROUP BY Patient_ID, Doctor_ID, Hospital_ID, Admission_ID, Medication_ID, Test_Result_ID, Admission_Date, Discharge_Date, Billing_Amount
HAVING COUNT(*) > 1;





-- Q1. What is the total number of patient categorised by blood type
create view blood_type as(
select blood_type, count(distinct patient_id) as total_patient
from dim_patient
group by 1);



-- Q2. Avegrage Billing amount based on patient gender
create view gender_type as(
select
	p.gender,
	round(avg(a.billing_amount), 2) as avg_bil
from dim_patient p
left join Fact_Admissions a on a.patient_id = p.patient_id
group by 1);


-- Q3. What is the top 3 highest billing amount for each doctor?
CREATE OR REPLACE view Topdoctor as(
with DoctorBilling as (
select Doctor_ID,
       sum(billing_amount) as Total_billing
from fact_admissions
group by Doctor_ID
),
RankedDoctor as (
select Doctor_ID,
	Total_billing,
    Rank() Over(order by Total_billing desc) as Ranked
from DoctorBilling
)
select r.Doctor_ID,d.doctor_name, r.Total_billing, r.Ranked
from RankedDoctor r
join dim_doctor as d on d.doctor_id = r.doctor_id
where Ranked <= 3);


-- Q4. What are the top 3 hospitals with the highest total billing amounts?
CREATE OR REPLACE VIEW TopHospitals AS (
    WITH HospitalBilling AS (
        SELECT 
            fa.Hospital_ID, 
            h.Hospital_Name, 
            SUM(fa.Billing_Amount) AS Total_Billing
        FROM Fact_Admissions fa
        JOIN Dim_Hospital h ON fa.Hospital_ID = h.Hospital_ID
        GROUP BY fa.Hospital_ID,h.Hospital_Name
    ),
    RankedHospitals AS (
        SELECT 
            Hospital_ID, 
            Hospital_Name, 
            Total_Billing,
            RANK() OVER (ORDER BY Total_Billing DESC) AS Ranked
        FROM HospitalBilling
    )
    SELECT 
        Hospital_ID, 
        Hospital_Name, 
        Total_Billing
    FROM RankedHospitals
    WHERE Ranked <= 3
);

