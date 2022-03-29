                                                                  ABC Bank
                                                                  
   Introduction: 
                                                                  
Introducing a ABC bank, this Bank want to move their processes and data onto the cloud. Their data resides in S3, where they have the details of the bank customer and the transaction detail.In this project we have created ETL pipeline in spark technology that extracts the data from S3 and we create a dataframe in Pyspark technology, and transforms data into a set of dimensional tables. Project Datasets We have worked with two datasets that reside in S3. 

The S3 links for each: 
• Customer data: s3://prateek-project/Bank Customer Details.csv
          The Bank customer details.csv file contain 10005 records.
           The Bank.csv file contain 1045002 records.
           
• Transaction data: s3://prateek-project/Bank.csv

The Data get stored in diffrent folder the link for that in S3 is:

Output_data : "s3://prateek-project/output_res"

* The count is mentioned in the jupyter file and a screenscot is attached

Technology Used:

S3: The data holds in the cloud where we can store the any amount of data we want.

Pyspark : The technology used is because it can handle a large amount of data and we can create dataframe which can easily be used to work on those and create other tables and saved them in diffrent files.

Schema for ABC bank : 

Fact Table:

Bank_trans - records in event data associated with Bank transactions i.e. records with Trans_id, Cust_id , Account_No, Action , DATE ,VALUE_DATE , Acc_type


Dimension Tables : 

Customer - Customer in the bank and the columns are : Cust_id, Gender, Education, Marital_Status, Age, Account_No

Accounts - The Account details and the columns are : Account_No , Acc_Type, Months_On_Bank, Credit_Score, Credit_Card, Estimated_Income

Bank_log_details - In this the transaction details in account : Account_No, DATE,TRANS_DETAILS,CHQ_NO,VALUE_DATE,WITHDRAW_AMT,DEPOSIT_AMT,BALANCE_AMT,Tras_id

There is one Fact table and three dimension table as mentioned above.The fact table contain the transaction detail that in which account the transaction happened and which customer associated with it. the fact table tells us about the transaction detail and which customer and which account the transaction happened.
The schema is been attached in the github.

The Data Quality CheckUps:

There is a data quality for bank_customer  files where we check for the data type if the data type is correct otherwise the data goes to a corrupt file.

We are also checking if the data entered in the tables is correct no of data that was in the files or not.


The steps of the process:

1. We create a schema for the Bank Customer Details.csv file which is a check if the data entered is in correct format.

2. We read the bank.csv file and there is bad record path which save the bad records in diffrent file.

3 .We make two diffrent dataframe for customer and account from the file and we save the file diffrently.

4. After this we bring second file bank.csv and we remove the null values from it with empty string.

5. After that we made a dataframe and added new column where we add if it's deposit or withdrawl.

6. Aftr that we make a dimension table where we copy the data from both input files and make a new dataframe and save that as a new file.


How often the data should be updated and why :

The data should we updated every day as there are new customer and transaction every day. 

Use Case of this data model:

This project have the details of the customer which can be used to count the no of customers in the bank.

Select count(*) from customers.

This can also tell about the balace present on the customer account and can you can also get the bank statement or the transaction details of the customer.

Select * from Bank_log_details where Account_No = '4998365327'

This will tell all the transaction in account of '4998365327'.

Scenerio : 

If the data gets increased by 100x then we can use the EMR cluster where we can use no of worker nodes which will help to process a large file of data.

If we need to run the file at 7 am we can define it in Airflow dag where we can make run everyday in 7am.

The files get saved diffrently and they can be saved into the database in amazon RDS by making a diffrent pipeline. Once the data get stored in RDS then any amount of user can access the data.


The project template includes three files excluding this file:

etl.py : In this file we have read data from S3, processes that data using Spark, and writes them back to S3.

     1.	In this we Implement the logic in to load data from json files residing in S3 to panda dataframe using spark.

     2.	After that we Implement the logic in etl.py to load data from Panda to analytics tables.

     3.	After that the table are loaded back to S3.

Dl.cfg:  Contains our AWS credentials

Untitled 5.ipynb  : This file is the testing copy of the etl file which contain the jupyter notebook file where the whole etl is made and run.




The Check is present where we see if the data is correct or not and we store the corrupt records in diffrent file
It's present for the both input files.
