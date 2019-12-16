package com.javaSpark.dataFrameExample;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
}
