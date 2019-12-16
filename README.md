# Data Pipeline case study in Spark Java
In this exercise we will use two input files:
1.	Data set started streams which contains the following fields (dt, time, device_name, house_number,user_id,country_code,program_title, season, season_episode, genre,product_type).
2.	Data set whatson which contains the following fields (dt, house_number, title,product_category, broadcast_right_region, broadcast_right_vod_type, broadcast_right_start_date, broadcast_right_end_date).

Tasks:
1.	Sales and rentals broadcast rights:
•	We need to find the broadcast rights for a title to be able to expose it to the analytics team for further analysis. This should be for Product Types: TVOD and EST. Matching on most recent date for whatson data and joining based on the house_number and country. 
•	Input: whatson.csv and started_streams.csv
•	Expected Output:
	dt,
	time, 
	device_name, 
	house_number,
	user_id,
	country_code,
	program_title, 
	season, 
	season_episode, 
	genre,
	product_type, 
	broadcast_right_start_date, 
	broadcast_right_end_date

2.	Product and user count:
•	We need to know how many watches a product is getting and how many unique users are watching the content, in what device, country and what product_type . 
•	Input: started_streams.csv
•	Expected output:
	dt,
	program_title, 
	device_name, 
	country_code, 
	product_type, 
	unique_users,
	content_count

3.	Genre and time of day: 
•	We need a list with the most popular Genre and what hours people watch?
•	Input: started_streams.csv
•	Expected output:
	watched_hour,
	genre,
	unique_users


### Prerequisits to execute the steps below:
   1) Maven is installed in your environment
   2) Spark is installed with 2+ version to execute spark-submit
   3) Java version 1.8 is there in the system 
   
## Step 1: 
### Clone the project
```
git clone https://github.com/sambhatt25/JavaSparkDataFrame.git

cd JavaSparkDataFrame
```

## Step 2: Build the code and package to jar file
### Run the below command, make sure you have mvn installed and spark installed in your system
```
mvn clean package
```

## Step 3: find the jar file created on successful build in the target directory of the project
```
ls target/*.jar
```

## Step 4: Execute application with below command: 
### You can configure i,e --executor-cores 2 --executor-memory 16G as per your custer configurations
### Please provide the whatson file path as the first argument and streams file as the second argument during execution
```
spark-submit --class com.javaSpark.dataFrameExample.DataFrameOp target\dataFrameExample-0.0.1-SNAPSHOT.jar whatson.csv started_streams_new.csv
```
### Please wait for the results as the LogLevel set to ERROR
