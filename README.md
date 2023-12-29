# Udacity_Data_Engineer_Nanodegree_Capstone_Project
My name is Aseem Narula, I am currently working as a Data Engineer at NatWest Group. I have undertaken the Data Engineer Nanodegree. In this module, I will be talking about my capstone project — US Airports and Immigration- Data Integration and ETL Data Pipeline.

**Introduction**
**Project: US Airports and Immigration- Data Integration and ETL Data Pipeline**
**Project Summary**
The main aim of this project is to integrate I94 immigration data, world temperature data and US demographic data with ETL data pipelines to build the data warehouse with fact and dimension tables to derive and generate the data insights for the business reporting.

In the data model, we will work on the STAR schema where I have combined the US Immigration data with the cities demographics along with the dimensions coming from the Airports, Countries, Temperatures data etc. which are optimized for the OLAP cube queries i.e. slicing and dicing if we wish to further expand the project to scale up with the additional resources.

We will first start with the details about the brief summary of the ETL steps that I will be performing to complete this project keeping the project rubrics in our mind.

Here we with the steps as follows —

**Scope the Project and Gather Data**

Since the scope of this US Immigration data project will be highly dependent on the data.

The following datasets are included in the project workspace.

**I94 Immigration Data**: This data comes from the US National Tourism and Trade Office. Each report contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries). https://travel.trade.gov/research/reports/i94/historical/2016.html

**World Temperature Data**: This dataset came from Kaggle. https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data

**U.S. City Demographic Dat**a: This data comes from OpenSoft. https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/

**Airport Code Table**: This is a simple table of airport codes and corresponding cities. https://datahub.io/core/airport-codes#data

Conceptual Data Model
![[STAR Schema](https://github.com/aseemnarula1/Udacity_Data_Engineer_Nanodegree_Capstone_Project/blob/main/Conceptual_Data_Model.png)



**Note** — Since the volume of the dataset is high, I have filtered the dataset when being pulled from the filtered by use of the sub-set smaller data set for my capstone project.

**GitHub Link** — https://github.com/aseemnarula1/Udacity_Data_Engineer_Nanodegree_Capstone_Project

**Medium Blog** - https://aseemnarula.medium.com/udacity-data-engineer-nanodegree-capstone-project-us-airports-and-immigration-data-fb146491b853

**Reference Links** 
1. Python Pandas Functions - https://pandas.pydata.org/docs/reference/general_functions.html
2. Pandas CSV Read - https://www.w3schools.com/python/pandas/pandas_csv.asp
3. Pandas Head Function - https://www.w3schools.com/python/pandas/ref_df_head.asp
4. Splitting the Code Variable in SAS format - https://communities.sas.com/t5/SAS-Programming/Splitting-a-city-and-zip-code-into-2-variables/td-p/435156
5. Descriptive Statistics Analysis - https://datatofish.com/descriptive-statistics-pandas/
6. STAR Schema for Data Modeling - https://www.geeksforgeeks.org/star-schema-in-data-warehouse-modeling/
7. PySpark SQL Function - https://sparkbyexamples.com/pyspark/pyspark-sql-with-examples/
8. Data Quality Checks in Pandas - https://www.telm.ai/blog/9-data-quality-checks-you-can-do-with-pandas/

**Acknowledgement**

All the datasets of US Immigration & Airports data used in this Capstone Project are provided through Udacity and are used for my project with Udacity Data Engineer Nanodegree and reference links are also provided where the docs are referred.
