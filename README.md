# sample_ques
hi Everyone, Hands on for today
Hands-On Lab: Logistics & Fleet Delivery Analytics
Objective: Build an end-to-end data analytics platform in Microsoft Fabric using a Medallion architecture (Bronze, Silver, Gold). You will implement a metadata-driven ingestion pipeline, cleanse data using PySpark, and build a dimensional model to calculate delivery KPIs.
Development Standard: While raw data will come in as-is, everything you create or transform (table names, column names, variables) must be in strictly formatted snake_case from the Bronze Delta tables onward.
Phase 1: Workspace Setup & Metadata Creation
Your first task is to set up your environment and prepare the raw data for a dynamic pipeline.
Environment Setup: Create a Fabric Workspace and a Lakehouse. Set up a logical folder structure in the "Files" section to represent your Bronze, Silver, and Gold layers.
Analyze the Raw Data: You have been provided a single Excel workbook (Logistics_Source.xlsx) containing two data sheets: Deliveries and Vehicles.
Create the Control Sheet: Add a new sheet to this workbook named metadata (or similar).
 
Design a schema in this sheet that will allow a data pipeline to iterate over the data sheets.
Hint: Your pipeline will need to know the name of the source sheet and the desired name of the target table (remember the snake_case rule for your target tables, e.g., raw_deliveries).
Upload: Upload this finalized workbook to your Bronze folder.
Phase 2: Metadata-Driven Ingestion (Data Pipelines)
Goal: Dynamically load all data sheets into structured Delta tables in the Bronze layer of your Lakehouse.
Read the Metadata: Create a Data Pipeline. Use an activity to read your newly created control sheet.
Iterate and Load: Pass the metadata output into a looping activity. Inside the loop, configure a Copy Data activity to:
Read the specific sheet defined by the current iteration.
Write the data as a Delta table into the Tables section of your Lakehouse, naming it dynamically based on your control sheet.
Phase 3: Cleansing & Standardization (PySpark - Silver Layer)
Goal: Cleanse the raw tables, enforce naming standards, and prepare the data for modeling.
Create a PySpark Notebook, attach it to your Lakehouse, and apply the following business rules to create your Silver tables (silver_deliveries, silver_vehicles):
Standardize Schema: Rename all columns in the dataframes to conform to snake_case (e.g., Distance_Miles becomes distance_miles, Delay_Mins becomes delay_in_minutes).
Handle Missing Data (Deliveries):
If driver_name is null, replace it with "Unknown".
If distance_miles is null, replace it with 0.0.
If delay_in_minutes is null, replace it with 0.
Data Quality Filter: Drop any records where the delivery status is "Cancelled".
Save: Write the cleansed DataFrames to the Lakehouse as Delta tables in overwrite mode.
Phase 4: Dimensional Modeling & KPIs (T-SQL - Gold Layer)
Goal: Build a Star Schema using the SQL Analytics Endpoint and calculate the required business KPIs.
Build the Star Schema: Use T-SQL to create your final Gold tables from your Silver tables.
Dimension Table: Create dim.vehicles. This table contains the descriptive attributes for the fleet.
Target Schema:
vehicle_id (String/Varchar - Foreign Key target)
vehicle_type (String/Varchar)
registration_year (Integer)
capacity_tons (Float/Decimal)
Fact Table: Create fact.deliveries. Alongside the standard delivery metrics, engineer a new binary column named is_on_time (1 if delay_in_minutes is 0, otherwise 0).
Target Schema:
trip_id (String/Varchar - Primary Key)
vehicle_id (String/Varchar - Foreign Key to gold_dim_vehicles)
driver_name (String/Varchar)
trip_date (Date)
distance_miles (Float/Decimal)
status (String/Varchar)
delay_in_minutes (Integer)
is_on_time (Integer - New Engineered Column)
Calculate KPIs: Write the T-SQL queries to generate the final reports:
KPI 1: On-Time Delivery Rate (Percentage of total deliveries completed without delays).
KPI 2: Average Distance per Vehicle Type (Join your Fact and Dim tables to aggregate this metric).
 
