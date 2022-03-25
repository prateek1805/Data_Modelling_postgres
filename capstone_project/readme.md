                                                                  ABC Bank
                                                                  
Introducing a ABC bank, this Bank want to move their processes and data onto the cloud. Their data resides in S3, in a directory of CSV logs on user activity on the app, as well as a directory with CSV metadata on the songs in their app. In this project we have created ETL pipeline that extracts the data from S3 copy it into staging tables in Redshift, and transforms data into a set of dimensional tables. Project Datasets We have worked with two datasets that reside in S3. 
The S3 links for each: 
• Customer data: s3://prateek-project/Bank Customer Details.csv
• Transaction data: s3://prateek-project/Bank.csv

The Data get stored in diffrent folder the link for that in S3 is:

Output_data : "s3://prateek-project/output_res"

Schema for sparkify App

Fact Table:

Bank_trans - records in event data associated with Bank transactions i.e. records with Trans_id, Cust_id , Account_No, Action , DATE ,VALUE_DATE , Acc_type


Dimension Tables : 

Customer - Customer in the bank and the columns are : Cust_id, Gender, Education, Marital_Status, Age, Account_No

Accounts - The Account details and the columns are : Account_No , Acc_Type, Months_On_Bank, Credit_Score, Credit_Card, Estimated_Income

Bank_log_details - In this the transaction details in account : Account_No, DATE,TRANS_DETAILS,CHQ_NO,VALUE_DATE,WITHDRAW_AMT,DEPOSIT_AMT,BALANCE_AMT,Tras_id


The project template includes two files excluding this file:

etl.py : In this file we have read data from S3, processes that data using Spark, and writes them back to S3.

     1.	In this we Implement the logic in to load data from json files residing in S3 to panda dataframe using spark.

     2.	After that we Implement the logic in etl.py to load data from Panda to analytics tables.

     3.	After that the table are loaded back to S3.

Dl.cfg:  Contains our AWS credentials


The Check is present where we see if the data is correct or not and we store the corrupt records in diffrent file
It's present for the both input files.
