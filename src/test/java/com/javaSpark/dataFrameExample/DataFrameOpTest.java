package com.javaSpark.dataFrameExample;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import org.apache.spark.sql.AnalysisException;

public class DataFrameOpTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public DataFrameOpTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( DataFrameOpTest.class );
    }
   

    public void testApp()
        {
	        assertTrue( true );
	}


    SparkSession spark = SparkSession
  	      .builder()
  	      .appName("DFAssignment")
  	      .master("local")
  	      .getOrCreate();
    
    String whatsOnFilePath=System.getProperty("user.dir") + "/files/whatsonSample.csv";
    String streamsFilePath=System.getProperty("user.dir") + "/files/streamsSample.csv";
    
    
    /**
     * @return Test the createDataFrame function
     */
    
    public void testAppcreateDataFrame() throws AnalysisException
    {
    	        
    	Dataset<Row> whatsonDF= DataFrameOp.createDataFrame(spark, whatsOnFilePath);
    	
    	assertTrue( whatsonDF.count() >= 0 );
    }
    
    /**
     * @return Test the findSalesAndBroadcastRights function
     */
    
    public void testAppfindSalesAndBroadcastRights() throws AnalysisException
    {
    	        
    	Dataset<Row> whatsonDF= DataFrameOp.createDataFrame(spark, whatsOnFilePath);
    	Dataset<Row> streamsDF=DataFrameOp.createDataFrame(spark,streamsFilePath);
    	   	
    	assertTrue( DataFrameOp.findSalesAndBroadcastRights(whatsonDF,streamsDF).count() >= 0 );
    }  
   
    /**
     * @return Test the findProductAndUserCount function
     */
    
    public void testAppfindProductAndUserCount() throws AnalysisException
    {
    	        
    	Dataset<Row> streamsDF=DataFrameOp.createDataFrame(spark,streamsFilePath);
    	   	
    	assertTrue( DataFrameOp.findProductAndUserCount(streamsDF).count() >= 0 );
    }
    
    /**
     * @return Test the findGenreAndTimeOfDay function
     */
    
    public void testAppfindGenreAndTimeOfDay() throws AnalysisException
    {
    	        
    	Dataset<Row> streamsDF=DataFrameOp.createDataFrame(spark,streamsFilePath);
    	   	
    	assertTrue( DataFrameOp.findGenreAndTimeOfDay(streamsDF).count() >= 0 );
    }
    
}
