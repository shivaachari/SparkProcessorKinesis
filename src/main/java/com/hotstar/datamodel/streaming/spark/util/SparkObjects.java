package com.hotstar.datamodel.streaming.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkObjects {

	static private transient SparkConf sparkConfig = null;
	 static private transient JavaStreamingContext jssc  = null;
	 static private transient SparkSession spark = null;
	 static private transient SQLContext sqlContext = null;
	 //private static String appName;
	 
	 private SparkObjects() {}
	 
	 static private boolean isInitialised = false;
	 
	 public static void initialize(String appName) throws Exception {
		 isInitialised = true;
			getSparkConf(appName, null);
			 
	 }
	 
	 public static void initialize(String appName, String master) throws Exception {
		 isInitialised = true;
				getSparkConf(appName, master);
				
	 }
	  
	  static public SparkConf getSparkConf(String appName, String master) throws Exception {
	    // Setup the Spark config and StreamingContext
		if(!isInitialised)
			throw new Exception("Spark Context not initialized, check if sparkObjects are initialized");
		  
		if (sparkConfig == null) {
			sparkConfig = new SparkConf().setAppName(appName);
			if (master != null && "".equals(master))
				sparkConfig.setMaster(master);
		}
		return sparkConfig;
	  }
	  
	  static public JavaStreamingContext getJavaStreamingContext( Duration batchInterval) throws Exception  {
		  if(!isInitialised)
				throw new Exception("Spark Context not initialized, check if sparkObjects are initialized");
	     if (jssc == null)
	    	 jssc = new JavaStreamingContext(sparkConfig, batchInterval);
	     return jssc;
	  }
	  
	  static public SparkSession getSparkSession(SparkConf sparkConf) throws Exception  {
		  if(!isInitialised)
				throw new Exception("Spark Context not initialized, check if sparkObjects are initialized");
		  
		  if (spark == null)
	     spark = SparkSession
	    		  .builder()
	    		  .config(sparkConf)
	    		  //.appName(appName)
	    		 // .config("spark.sql.warehouse.dir", warehouseLocation)
	    		  .enableHiveSupport()
	    		  .getOrCreate();
		  
		  return spark;
	  }
	  
	  static public SparkSession getSparkSession() throws Exception  {
		  if(!isInitialised)
				throw new Exception("Spark Context not initialized, check if sparkObjects are initialized");
		  
		  if (spark == null)
	     spark = SparkSession
	    		  .builder()
	    		  .config(sparkConfig)
	    		  //.appName(appName)
	    		 // .config("spark.sql.warehouse.dir", warehouseLocation)
	    		  .enableHiveSupport()
	    		  .getOrCreate();
		  
		  return spark;
	  }
	    static public SQLContext getSQLContext(SparkSession spark) throws Exception  {
	    	if(!isInitialised)
				throw new Exception("Spark Context not initialized, check if sparkObjects are initialized");
	    	
	    	if (spark == null)
	    		sqlContext = new SQLContext(spark);
	    	return sqlContext;
	    }
	    
	    /*static private transient SQLContext instance = null;
		  static public SQLContext getSQLContext(SparkContext sparkContext) throws Exception  {
			  if(!isInitialised)
					throw new Exception("Spark Context not initialized, check if sparkObjects are initialized");
		    if (instance == null) {
		    	sparkContext.
		      instance = new SQLContext(sparkContext);
		    }
		    return instance;
		  }
	    
	    static private transient HiveContext hiveContext = null;
		  public static <T> HiveContext getHiveContext(JavaRDD<T> rdd) throws Exception  {
			  if(!isInitialised)
					throw new Exception("Spark Context not initialized, check if sparkObjects are initialized");
			  if (hiveContext != null) 
				  return hiveContext;
			  
			  SQLContext sqlContext1 = getSQLContext(rdd.context());	
			  //SQLContext sqlContext1 = sqlContext;
				
				HiveContext hiveContext = new HiveContext(sqlContext1.sparkContext());	
				
				hiveContext.setConf("hive.exec.dynamic.partition", "true");
				hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
				
				return hiveContext;
		  }
		  
		  public static <T> HiveContext getHiveContext() throws Exception  {
			  if(!isInitialised)
					throw new Exception("Spark Context not initialized, check if sparkObjects are initialized");
			  if (hiveContext != null) 
				  return hiveContext; 
			 
				HiveContext hiveContext = new HiveContext(getSparkSession());	
				
				hiveContext.setConf("hive.exec.dynamic.partition", "true");
				hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
				
				return hiveContext;
		  }*/
	 
	/*  static private transient HiveContext hiveContext = null;
	  public static HiveContext getHiveContext(JavaRDD<Object[]> data) {
		  if (hiveContext != null) 
			  return hiveContext;
		  
		  SQLContext sqlContext1 = sparkUtil.getInstance(data.context());		
			
			HiveContext hiveContext = new HiveContext(sqlContext1.sparkContext());	
			
			hiveContext.sql("use shiva_ris_test");
			
			hiveContext.setConf("hive.exec.dynamic.partition", "true");
			hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
			
			hiveContext.udf().register("diffUdf", new UDF2<Long, Long, Integer>() {
				private static final long serialVersionUID = 5164534477833441211L;

				@Override
				public Integer call(Long beginTime, Long endTime) {
					if (beginTime == 0 || endTime == 0)
						return 0;
					else
						return (int) (endTime - beginTime);
				}
			}, DataTypes.IntegerType);
			
			return hiveContext;
	  }
	  */
	  
}
