use newdb;

#Create ChinaCancerPatients Table
create table ChinaCancerPatients(
PatientID varchar(200),
Gender varchar(200),
Age varchar(200),
Province varchar(200),
Ethnicity varchar(200),
TumorType varchar(200),
CancerStage varchar(200),
DiagnosisDate varchar(200),
TumorSize varchar(200),
Metastasis varchar(200),
TreatmentType varchar(200),
SurgeryDate varchar(200),
ChemoTherapySessions varchar(200),
RadiationSessions varchar(200),
SurvivalStatus varchar(200),
FollowUpMonths varchar(200),
SmokingStatus varchar(200),
AlcoholUse varchar(200),
GeneticMutation varchar(200),
Comorbidities varchar(200));

SHOW GLOBAL VARIABLES LIKE 'local_infile';

SET GLOBAL local_infile = 1;

# Load the data
load data local infile 'C:/Program Files/MySQL/MySQL Server 8.0/Uploads/china_cancer_patients_synthetic.CSV'
into table ChinaCancerPatients
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

#Check the load data status
select count(*) from ChinaCancerPatients;

SELECT * FROM newdb.chinacancerpatients;

# Create a new table with appropriate datatypes
CREATE TABLE cancer_patients_clean (
    PatientID VARCHAR(200) PRIMARY KEY,
    Gender VARCHAR(200),
    Age INT,
    Province VARCHAR(200),
    Ethnicity VARCHAR(200),
    TumorType VARCHAR(200),
    CancerStage VARCHAR(200),
    DiagnosisDate DATE,
    TumorSize FLOAT,
    Metastasis VARCHAR(200),
    TreatmentType VARCHAR(200),
    SurgeryDate DATE,
    ChemotherapySessions INT,
    RadiationSessions INT,
    SurvivalStatus VARCHAR(200),
    FollowUpMonths INT,
    SmokingStatus VARCHAR(200),
    AlcoholUse VARCHAR(200),
    GeneticMutation VARCHAR(200),
    Comorbidities VARCHAR(200)
);

# Insert data into new clean table with type conversion and cleaning
insert into cancer_patients_clean
select 
PatientID, 
case when lower(Gender) in ('male', 'female', 'other') then Gender
else 'Other'
end as Gender,
case when Age regexp '^[0-9]+$' and cast(Age as unsigned) between 0 and 120 then cast(Age as unsigned)
else null
end as Age,
Province,
Ethnicity,
TumorType,
case when CancerStage in ('I', 'II', 'III', 'IV') then CancerStage
else null
end as CancerStage,
case when DiagnosisDate regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then str_to_date(DiagnosisDate, '%Y-%m-%d')
else null
end as DiagnosisDate,
case when TumorSize regexp '^[0-9]+\\.?[0-9]*$' and cast(TumorSize as Decimal(10,2)) > 0 then cast(TumorSize as Decimal(10,2))
else null
end as TumorSize,
case when Metastasis in ('Yes', 'No') then Metastasis
else 'No'
end as Metastasis,
TreatmentType,
case when SurgeryDate regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then str_to_date(SurgeryDate, '%Y-%m-%d')
else null
end as SurgeryDate,
case when ChemotherapySessions regexp '^[0-9]+$' and cast(ChemotherapySessions as unsigned) >= 0 then cast(ChemotherapySessions as unsigned)
else 0
end as ChemotherapySessions,
case when RadiationSessions regexp '^[0-9]+$' and cast(RadiationSessions as unsigned) >= 0 then cast(RadiationSessions as unsigned)
else 0
end as RadiationSessions,
case when SurvivalStatus in ('Alive', 'Deceased') then SurvivalStatus
else null
end as SurvivalStatus,
case when FollowUpMonths regexp '^[0-9]+$' and cast(FollowUpMonths as unsigned) >= 0 then cast(FollowUpMonths as unsigned)
else null
end as FollowUpMonths,
case when SmokingStatus in ('Current', 'Former', 'Never') then SmokingStatus
else 'Unknown'
end as SmokingStatus,
case when AlcoholUse in ('None', 'Occasional', 'Regular') then AlcoholUse
else 'Unknown'
end as AlcoholUse,
ifnull(GeneticMutation, 'None') as GeneticMutation,
ifnull(Comorbidities, 'None') as Comorbidities
from ChinaCancerPatients where PatientID is not null and PatientID regexp '^CHN-[0-9]{5}$';

#Check Duplicates
create table duplicate_patients as 
select PatientID, count(*) as cnt from cancer_patients_clean group by PatientID having cnt > 1;

#check duplicate table count
select count(*) from duplicate_patients;

#deduplicate by keeping the row with the most recent DiagnosisDate
CREATE TABLE cancer_patients_dedup AS
WITH RankedRows AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY PatientID 
            ORDER BY DiagnosisDate DESC, COALESCE(Age, 0) DESC, COALESCE(TumorSize, 0) DESC
        ) AS rn
    FROM cancer_patients_clean
)
SELECT *
FROM RankedRows
WHERE rn = 1;

#Data quality checks - Check for missing critical values
SELECT 'Missing PatientID' AS Issue, COUNT(*) AS Count
FROM chinacancerpatients WHERE PatientID IS NULL OR PatientID = ''
UNION
SELECT 'Missing TumorType' AS Issue, COUNT(*) AS Count
FROM chinacancerpatients WHERE TumorType IS NULL OR TumorType = ''
UNION
SELECT 'Invalid Age' AS Issue, COUNT(*) AS Count
FROM chinacancerpatients WHERE Age NOT REGEXP '^[0-9]+$' OR CAST(Age AS UNSIGNED) > 120
UNION
SELECT 'Invalid TumorSize' AS Issue, COUNT(*) AS Count
FROM chinacancerpatients WHERE TumorSize NOT REGEXP '^[0-9]+\\.?[0-9]*$' OR CAST(TumorSize AS DECIMAL(10,2)) <= 0;

#Aggregations for insights - Distribution by TumorType and CancerStage
SELECT TumorType, CancerStage, COUNT(*) AS PatientCount
FROM cancer_patients_dedup
GROUP BY TumorType, CancerStage
ORDER BY TumorType, CancerStage;

#Average TumorSize by TumorType
SELECT TumorType, ROUND(AVG(TumorSize), 2) AS AvgTumorSize
FROM cancer_patients_dedup
WHERE TumorSize IS NOT NULL
GROUP BY TumorType;

#Survival rate by CancerStage
SELECT 
    CancerStage, 
    SUM(CASE WHEN SurvivalStatus = 'Alive' THEN 1 ELSE 0 END) / COUNT(*) * 100 AS SurvivalRate
FROM cancer_patients_dedup
WHERE CancerStage IS NOT NULL
GROUP BY CancerStage;

#Treatment patterns
SELECT 
    TreatmentType, 
    COUNT(*) AS PatientCount, 
    SUM(CASE WHEN SurgeryDate IS NOT NULL THEN 1 ELSE 0 END) AS SurgeryCount,
    AVG(ChemotherapySessions) AS AvgChemoSessions,
    AVG(RadiationSessions) AS AvgRadiationSessions
FROM cancer_patients_dedup
GROUP BY TreatmentType;

#Impact of SmokingStatus on Survival
SELECT 
    SmokingStatus, 
    SUM(CASE WHEN SurvivalStatus = 'Alive' THEN 1 ELSE 0 END) / COUNT(*) * 100 AS SurvivalRate
FROM cancer_patients_dedup
WHERE SmokingStatus != 'Unknown'
GROUP BY SmokingStatus;

#Export cleaned data for further analysis
CREATE TABLE cancer_patients_final AS
SELECT * FROM cancer_patients_dedup;

