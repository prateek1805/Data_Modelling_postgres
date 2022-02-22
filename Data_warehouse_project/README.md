                                                    Sparkify
Introduction
	A music streaming app, Sparkify want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
In this project we have created ETL pipeline that extracts the data from S3 copy it into staging tables in Redshift, and transforms data into a set of dimensional tables.
Project Datasets
We have worked with two datasets that reside in S3. The S3 links for each:
•	Song data: s3://udacity-dend/song_data
•	Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json

Schema for sparkify App

Fact Table
1.	songplays - records in event data associated with song plays i.e. records with page NextSong
•	songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
Dimension Tables
1.	users - users in the app
a.	user_id, first_name, last_name, gender, level
2.	songs - songs in music database
a.	song_id, title, artist_id, year, duration
3.	artists - artists in music database
a.	artist_id, name, location, lattitude, longitude
4.	time - timestamps of records in songplays broken down into specific units
a.	start_time, hour, day, week, month, year, weekday


Project Template
The project template includes Five files excluding this file:

Create_table.py : In this file we have created the  fact and dimension tables for the star schema in Redshift.

	In this File we are creating the tables for redshift cluster. We import the sql_queries file and the  execute them one by one.

1.	First we create the database with:

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()

2.	Firstly there are SQL drop statement to drop the  table if any of them is present 

3.	Then there is function create_tables() that is used to create the staging, fact and dimension tables.

4.	This way we can run this file whenever we want to reset our database and test our ETL pipeline.

etl.py : This file is used to load data from S3 into staging tables on Redshift and then process that data into the analytics tables on Redshift.
1.	In this we Implement the logic in to load data from S3 to staging tables on Redshift.
2.	After that we Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift cluster.

3.	The  etl.py is tested after running create_tables.py and running the analytic queries on your Redshift database to import the data from S# to tables in database.

Sql_queries.py : This file contains all the SQL statements, which will be imported into the two other files above.


Creating_cluster :  This file is used to create a cluster in the AWS. The cluster is created through python and it import dwh.cfg. In this file below there is code to delete the cluster also.

1.	We start the file with importing panda, boto3 and json.
2.	Then we import dwh.cfg file.
3.	The resources are created like EC2,IAM,redshift.
4.	Then we create the new role and attach the policy to access S3.
5.	Then we create the redshift cluster and then describe it to see the status
6.	Then we Open an incoming TCP port to access the cluster endpoint
7.	Then we connect to cluster and at last we clear the resources when our work is done.

dwh.cfg : This file contain the data required to create cluster or the S3 and  other parameters required in this project.