package com.javaSpark.dataFrameExample;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.*;


public class DataFrameOp {

 private static Dataset<Row> createDataFrame(SparkSession spark,String csvPath) throws AnalysisException {
	    /*
	    This is to create the dataframe with a csv file
	    */
	    Dataset<Row> df = spark.read().option("header", "true").csv(csvPath);
	    return df;
	}
	
 private static void findSalesAndBroadcastRights(Dataset<Row> whatsonDF, Dataset<Row> startedStreamsDF) throws AnalysisException {
	 
	 /*
	 This function matches the whatson and streams on basis of house number to find the broadcast rights for each house 
	 */
	 
	 //This dataframe finds the latest date in whatson
	 Dataset<Row> whatsonMaxDtDF=whatsonDF.groupBy("house_number").agg(max("dt").as("dt"))
			                   .withColumnRenamed("house_number", "house_number_whatson").withColumnRenamed("dt", "dt_whatson")
			                   .join(whatsonDF, col("house_number_whatson").equalTo(whatsonDF.col("house_number"))
			                		   .and(col("dt_whatson").equalTo(whatsonDF.col("dt")))).drop("house_number").drop("dt");
	 //whatsonMaxDtDF.show();
	 
	 //This dataframe is the final dataframe where it matches with the streams data on basis of house number
	 //Filters product types as per the given criteria
	 Dataset<Row> broadcastRightsDF = startedStreamsDF.filter("product_type = 'tvod' OR product_type = 'est'")
			                          .join(whatsonMaxDtDF, startedStreamsDF.col("house_number").equalTo(whatsonMaxDtDF.col("house_number_whatson")))
			                          .select("dt_whatson","time","device_name","house_number","user_id","country_code","program_title","season","season_episode","genre","product_type","broadcast_right_start_date","broadcast_right_end_date")
			                          .withColumnRenamed("dt_whatson", "dt")
			                          ;
	 System.out.println( "1.	Sales and rentals broadcast rights results======================================>" );
	 
	 //Printing sample output
	 broadcastRightsDF.show(100,false);
 }
 
 private static void findProductAndUserCount(Dataset<Row> startedStreamsDF) throws AnalysisException {
	 //Finding the unique user count for each program
	 Dataset<Row> UserCountDF=startedStreamsDF.dropDuplicates("program_title","user_id").groupBy("program_title").agg(count("user_id").as("unique_users")).cache()
			                          ;
	 
	 //Finding the total count of watches for each program
	 Dataset<Row> ProductAndUserCountDF=startedStreamsDF.groupBy("program_title").agg(count("user_id").as("content_count"))
			                           .join(startedStreamsDF,"program_title").dropDuplicates("program_title")			                           
			                           .select("dt","program_title","device_name","country_code","product_type","content_count")
			                           ;
	 System.out.println( "2.	Product and user count results======================================>" );
	 
	 //Final dataframe where it displays all the results together
	 UserCountDF.join(ProductAndUserCountDF,"program_title").select("dt","program_title","device_name","country_code","product_type","unique_users","content_count").show(100,false); 
	 
 }
 
 private static void findGenreAndTimeOfDay(Dataset<Row> startedStreamsDF) throws AnalysisException {
	 //Finding unique users per genre
	 Dataset<Row> GenreUniqUserDF=startedStreamsDF.dropDuplicates("genre","user_id").groupBy("genre").agg(count("user_id").as("unique_users")).cache();
	 
	 //Extracting average hour which will identify the an approx hour in a day to estimate which hour each genre is watched most
	 Dataset<Row> GenreHourDF=startedStreamsDF.withColumn("watched_hour",  from_unixtime(unix_timestamp(col("time"), "HH:mm:ss"), "HH"))
	                .groupBy("genre")
	                .agg(round(avg("watched_hour")).as("watched_hour"))
	                .dropDuplicates("genre");
	
	 System.out.println( "3.	Genre and time of day results======================================>" );
	 
	 //Final dataframe to display the end result together
	 GenreUniqUserDF.join(GenreHourDF,"genre")
	                .select("watched_hour","genre","unique_users").sort(desc("unique_users")).show(false);
	 
	 
 }
 
 public static void main(String[] args) throws AnalysisException {
	   
	    SparkSession spark = SparkSession
	      .builder()
	      .appName("DFAssignment")
	      .master("local")
	      .getOrCreate();
	    
	    spark.sparkContext().setLogLevel("ERROR");

	    
        //String whatsonCSVPath="C:\\Users\\Sam\\Desktop\\assignments\\whatson.csv";
	    String whatsonCSVPath= args[0];
        //String streamsCSVPath="C:\\Users\\Sam\\Desktop\\assignments\\started_streams_new.csv";
	    String streamsCSVPath = args[1];
        
        Dataset<Row> whatsonDF=createDataFrame(spark,whatsonCSVPath);
        Dataset<Row> streamsDF=createDataFrame(spark,streamsCSVPath);
        
        System.out.println( "WhatsOn sample data======================================>" );
        whatsonDF.show(false);
        
        System.out.println( "Streams sample data======================================>" );
        streamsDF.show(false);
        
       
        findSalesAndBroadcastRights(whatsonDF,streamsDF);
        
        findProductAndUserCount(streamsDF);

        findGenreAndTimeOfDay(streamsDF);
        
	    spark.stop();
	  }	
	
	
}
