import findspark
findspark.init()
findspark.find()

from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import when
from pyspark.sql.functions import col,lit
from pyspark.sql.types import StructType,StructField,StringType,LongType,IntegerType
from pyspark.sql.functions import monotonically_increasing_id

"""
This Function create a Spark Session which will act as a entry point to the spark and need to be called everytime we want to execute it.

"""

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \        
           .builder \
           .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
           .getOrCreate()
    
    return spark



def process_customer_data(spark,input_data,output_data):
    
     """
    Description:
        In this module we will process the Customer data files and create extract Customer table and Account table data from it.
    :param spark: a spark session instance
    
    Schem define the schema for the bank customer detail file and add a colum corrupt record to take out the corrupt record that are present        in the file.
    
    """
    
    Schem = StructType(
        [
            
            StructField("Account_No",LongType(),False),\
            StructField("Age",IntegerType(),False),\
            StructField("Gender",StringType(),False),\
            StructField("Education",StringType(),False),\
            StructField("Marital_Status",StringType(),False),\
            StructField("Acc_Type",StringType(),False),\
            StructField("Months_On_Bank",IntegerType(),False),\
            StructField("Credit_Card",StringType(),False),\
            StructField("Credit_Score",IntegerType(),False),\
            StructField("Estimated_Income",StringType(),False),\
            StructField("Cust_id",IntegerType(),False),\
        ]
    )
    
    # In this the corrupt records get stored into a diffrent file if the data is not in correct schema
    
    Bank_df = (
      spark.read
            .option("badRecordsPath","/tmp/badRecordsPath") \
            .csv(os.path.join(input_data+"Bank Customer Details.csv"),header = True,schema = Schem)
    )
    
    
    # extracting columns to create Customer table
    
    Customer = Bank_df.select("Cust_id",
                      "Gender",
                      "Education",
                      "Marital_Status",
                       "Age",
                       "Account_No"
                    ).drop_duplicates()
    
    # extracting columns to create Account table

    Account = Bank_df.select("Account_No",
                    "Acc_Type",
                    "Months_On_Bank",
                    "Credit_Score",
                    "Credit_Card",
                    "Estimated_Income"
                    ).drop_duplicates()
    
    
    Customer.write.mode('overwrite').parquet(os.path.join(output_data+"customer/"))
    
    Account.write.mode('overwrite').parquet(os.path.join(output_data+"Account/"))
    
    
def process_transaction_data(spark,input_data,output_data):
    
    """
    Description:
           In this module we will process the Account transaction data files and create extract Bank_log_details table and Bank_trans table   data from it.
    :param spark: a spark session instance
    
    """

    df = spark.read \
                .option("badRecordsPath","/tmp/badRecordsPath1") \
                .csv(os.path.join(input_data+"Bank.csv"),header = True,inferSchema = True)
    
    df1 = df.na.fill("")
    
    # extracting columns to create Bank_log_details table
    
    Bank_log_details = df1.select("Account_No",
                              "DATE",
                              "TRANS_DETAILS",
                              "CHQ_NO",
                              "VALUE_DATE",
                              "WITHDRAW_AMT",
                              "DEPOSIT_AMT",
                              "BALANCE_AMT",
                              "Tras_id").drop_duplicates()
    
    Bank_log_details = Bank_log_details.withColumn("Action", \                    #Adding new column to tell if withdraw happen or deposit
       when((Bank_log_details.WITHDRAW_AMT !=""), lit("Withdraw"))\
     .otherwise(lit("Deposit")))
    
    Bank_log_details.write.mode('overwrite').partitionBy("Account_No")parquet(os.path.join(output_data+"Bank_log/"))
    
    
    #Reading the files which we stored in previous function as there column need to be used to create Bank_trans table 
    
    
    Customer_details = spark.read.parquet(os.path.join(output_path+'Customer/'))       
    
    Account_details = spark.read.parquet(os.path.join(output_path+'Account/'))
    
    # extracting columns to create Brank_trans table and join is happening because we want deatils from three diffrent dataframe
    
    Bank_trans = Bank_log_details.join(Customer_details, Bank_log_details.Account_No == Customer_details.Account_No, how='inner')\
                        .select(monotonically_increasing_id().alias("Trans_id"),\
                        col("Cust_id"),\
                        df1.Account_No,\
                        "Action",\
                        "DATE",
                        "VALUE_DATE"\
                        )
    Bank_trans = Bank_trans.join(Account_details, Bank_trans.Account_No == Account_details.Account_No, how='inner')\
                        .select(monotonically_increasing_id().alias("Trans_id"),\
                        col("Cust_id"),\
                        Bank_trans.Account_No,\
                        col("Action"),\
                        col("DATE"),
                        col("VALUE_DATE"),\
                        "Acc_type"
                        )
    Bank_trans.write.mode('overwrite').partitionBy("Account_No")parquet(output_path+'Bank_trans/')


def main():
    spark = create_spark_session()
    input_data = "s3://prateek-project/"
    output_data = "s3://prateek-project/output_res"
    
    process_customer_data(spark)    
    process_transaction_data(spark)


if __name__ == "__main__":
    main()