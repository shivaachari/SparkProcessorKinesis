package com.hotstar.datamodel.streaming.spark.processor;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.hotstar.datamodel.streaming.spark.struct.RowFieldsEnum;
import com.hotstar.datamodel.streaming.spark.struct.RowFieldsVO;
import com.hotstar.datamodel.streaming.spark.util.SparkObjects;

import scala.Tuple2;

/**
 * Consumes messages from a Amazon Kinesis streams and does wordcount.
 *
 * This example spins up 1 Kinesis Receiver per shard for the given stream.
 * It then starts pulling from the last checkpointed sequence number of the given stream.
 *
 * Usage: JavaKinesisWordCountASL [app-name] [stream-name] [endpoint-url] [region-name]
 *   [app-name] is the name of the consumer app, used to track the read data in DynamoDB
 *   [stream-name] name of the Kinesis stream (ie. mySparkStream)
 *   [endpoint-url] endpoint of the Kinesis service
 *     (e.g. https://kinesis.us-east-1.amazonaws.com)
 *
 *
 * Example:
 *      # export AWS keys if necessary
 *      $ export AWS_ACCESS_KEY_ID=[your-access-key]
 *      $ export AWS_SECRET_KEY=<your-secret-key>
 *
 *      # run the example
 *      $ SPARK_HOME/bin/run-example   streaming.JavaKinesisWordCountASL myAppName  mySparkStream \
 *             https://kinesis.us-east-1.amazonaws.com
 *
 * There is a companion helper class called KinesisWordProducerASL which puts dummy data
 * onto the Kinesis stream.
 *
 * This code uses the DefaultAWSCredentialsProviderChain to find credentials
 * in the following order:
 *    Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
 *    Java System Properties - aws.accessKeyId and aws.secretKey
 *    Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
 *    Instance profile credentials - delivered through the Amazon EC2 metadata service
 * For more information, see
 * http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html
 *
 * See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more details on
 * the Kinesis Spark Streaming integration.
 */
public final class JavaKinesisSparkProcessor2 { // needs to be public for access from run-example
 // private static final Pattern WORD_SEPARATOR = Pattern.compile(" ");
  private static final Pattern LINE_SEPARATOR = Pattern.compile(System.lineSeparator());

  public static void main(String[] args) throws Exception {
    // Check that all required args were passed in.
   /* if (args.length != 3) {
      System.err.println(
          "Usage: JavaKinesisWordCountASL <stream-name> <endpoint-url>\n\n" +
          "    <app-name> is the name of the app, used to track the read data in DynamoDB\n" +
          "    <stream-name> is the name of the Kinesis stream\n" +
          "    <endpoint-url> is the endpoint of the Kinesis service\n" +
          "                   (e.g. https://kinesis.us-east-1.amazonaws.com)\n" +
          "Generate data for the Kinesis stream using the example KinesisWordProducerASL.\n" +
          "See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more\n" +
          "details.\n"
      );
      System.exit(1);
    }*/

    // Set default log4j logging level to WARN to hide Spark logs
   // StreamingExamples.setStreamingLogLevels();
    
    String[] args1 = {"spark_processor", "ClientData_json_streams", 
            "kinesis.eu-west-1.amazonaws.com"};

    // Populate the appropriate variables from the given args
    String kinesisAppName = args1[0];
    String streamName = args1[1];
    String endpointUrl = args1[2];

    // Create a Kinesis client in order to determine the number of shards for the given stream
    AmazonKinesisClient kinesisClient =
        new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
    kinesisClient.setEndpoint(endpointUrl);
    int numShards =
        kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();


    // In this example, we're going to create 1 Kinesis Receiver/input DStream for each shard.
    // This is not a necessity; if there are less receivers/DStreams than the number of shards,
    // then the shards will be automatically distributed among the receivers and each receiver
    // will receive data from multiple shards.
    int numStreams = numShards;

    // Spark Streaming batch interval
    Duration batchInterval = new Duration(5*1000);

    // Kinesis checkpoint interval.  Same as batchInterval for this example.
    Duration kinesisCheckpointInterval = batchInterval;

    // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
    // DynamoDB of the same region as the Kinesis stream
    String regionName = RegionUtils.getRegionByEndpoint(endpointUrl).getName();

    // Setup the Spark config and StreamingContext
    SparkConf sparkConfig = new SparkConf().setAppName("JavaKinesisSparkProcessor")
    		.setMaster("local[*]");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);
    SparkSession sparkSession = SparkObjects.getSparkSession(sparkConfig);
    
   

    // Create the Kinesis DStreams
    List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numStreams);
    for (int i = 0; i < numStreams; i++) {
      streamsList.add(
          KinesisUtils.createStream(jssc, kinesisAppName, streamName, endpointUrl, regionName,
              InitialPositionInStream.LATEST, kinesisCheckpointInterval,
              StorageLevel.MEMORY_AND_DISK_2())
      );
    }

    // Union all the streams if there is more than 1 stream
    JavaDStream<byte[]> unionStreams;
    if (streamsList.size() > 1) {
      unionStreams = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
    } else {
      // Otherwise, just use the 1 stream
      unionStreams = streamsList.get(0);
    }
    
    unionStreams.foreachRDD(new VoidFunction<JavaRDD<byte[]>>(){
    	
    	/*
    	 * JavaRDD<Tuple2<Integer, Object[]>> splittedData = data.map(new Function<Object[],Tuple2<Integer, Object[]>>()
	    {
	        private static final long serialVersionUID = -2381522520231963249L;

	        @Override
	        public Tuple2<Integer, Object[]> call(Object[] ss7Data) throws Exception
	        {
	            // Split a row of data by commas (,).
	          //  String[] tokens = s.split(",");

	            // Integrate the three split elements to a ternary Tuple.
	            Tuple2<Integer, Object[]> person = new Tuple2<Integer, Object[]>(Integer.parseInt(ss7Data[2].toString()), ss7Data);
	            return person;
	        }
	    });
		
		isupDataRDD = splittedData.filter(new Function<Tuple2<Integer, Object[]>, Boolean>()
	    {
	        private static final long serialVersionUID = -4210609503909770492L;

	        @Override
	        public Boolean call(Tuple2<Integer, Object[]> ss7Data) throws Exception
	        {
	            // Filter the records of which the sex in the second column is female.
	            Boolean isIsupData = (ss7Data._1() == 2);
	            return isIsupData;
	        }
	    }).map(
	   		  new Function<Tuple2<Integer, Object[]>, Row>() {//int tempCount=0;
		 		    private static final long serialVersionUID = 8554347338412767649L;

		 			public Row call(Tuple2<Integer, Object[]> record) throws Exception {		    	
		 		      return RowFactory.create(record._2());
		 		    }
		 		  });;
	    @see org.apache.spark.api.java.function.VoidFunction#call(java.lang.Object)
    	 */
    	
    	

		@Override
		public void call(JavaRDD<byte[]> rdd) throws Exception {
			JavaRDD<Row> rowRDD = rdd.map(new Function<byte[], Row>(){

				@Override
				public Row call(byte[] lines) throws Exception {
					ArrayList<RowFieldsVO> rowArray = new ArrayList<RowFieldsVO>();
					try{
				    	  //System.out.println("############# In unionStreams.flatMap ");
				        String s = new String(lines, StandardCharsets.UTF_8);
				        Iterator<String> data = Arrays.asList(LINE_SEPARATOR.split(s)).iterator();
				        //System.out.println("############# In unionStreams.flatMap String s:"+ s);
				        
				        
				        
				        while(data.hasNext()) {
				        	 String text = data.next();
				        	// System.out.println("############# In unionStreams.flatMap String text:"+ text);
				        	 JsonObject jsonObject;
				        	 RowFieldsVO rowData;
							try{
					        	
				                 JsonReader reader = new JsonReader(new StringReader(text));
				                 reader.setLenient(true);
				                 jsonObject = new JsonParser().parse(reader).getAsJsonObject();
				                 System.out.println("############# In unionStreams.flatMap String jsonObject:"+ jsonObject);
				                 
				                 System.out.println("############# JSON userid: "+jsonObject.get("userid").getAsString());
				                 
				                 rowData = new RowFieldsVO(
						                		 "test",
						                		 jsonObject.get("userid").getAsString(),
						                		 jsonObject.get("firstname").getAsString(),
						                		 jsonObject.get("username").getAsString(),
						                		 jsonObject.get("email").getAsString(),
						                		 jsonObject.get("state").getAsString()
				                		 	);
				                 
				                 return RowFactory.create("test",
				                		 jsonObject.get("userid").getAsString(),
				                		 jsonObject.get("firstname").getAsString(),
				                		 jsonObject.get("username").getAsString(),
				                		 jsonObject.get("email").getAsString(),
				                		 jsonObject.get("state").getAsString());
				                 //rowArray.add(rowData);
				             }catch(Exception e2) {
				            	 e2.printStackTrace();
				             }
				        	 
				        }
				        
				        
				        }catch(Exception e) {
				        	e.printStackTrace();
				        }
				       return RowFactory.create(rowArray);
					//return (Row[]) rowArray.toArray();
				}
				
			});
			
			try{
				
				  HiveContext sqlContext = SparkObjects.getHiveContext(rdd);
				Dataset<Row> dataframe = sqlContext.createDataFrame(rowRDD, createSchema());
				
				//dataframe.registerTempTable("tempHiveTable");
				dataframe.createOrReplaceTempView("tempHiveTable");
				
				sparkSession.sql("select * from tempHiveTable").show();
			   }catch(Exception e) {
		        	e.printStackTrace();
		        }
		}
		
		
		//wordsDataFrame = hiveContext.createDataFrame(rowRDD, tableSchema)
    	
    });
    
    /*data = unionStreams.foreachRDD ((rdd: RDD[Array[Byte]], time: Time)  {

    	  // Split each line in each Dstream RDD by given delimiter and feed

    	   rowRDD = rdd.map(w => Row.fromSeq(new String(w).split(inputDelimiter)))  

    	  //create a DataFrame - For each DStream -> RDD for the batch window. This will basically convert a window of 
    	  //"batchInterval" size block of data on kinesis in to a dataframe.

    	   wordsDataFrame = hiveContext.createDataFrame(rowRDD, tableSchema)

    	    // Register the current dataFrame in the loop as table
    	  wordsDataFrame.registerTempTable(tempTableName)
    	  val sqlString = "select count(*) from "+tempTableName
    	     // Do a count on table using SQL and print it for each timestamp on console
    	     val wordCountsDataFrame = hiveContext.sql(sqlString)
    	     
    	     println(s"========= $time =========")
    	     wordCountsDataFrame.show()
    	   });
    */

    /*
     * Older version kept for reference
     * 
    // Convert each line of Array[Byte] to String, and split into words
    JavaDStream<String> records = unionStreams.flatMap(new FlatMapFunction<byte[], String>() {
      @Override
      public Iterator<String> call(byte[] line) {
    	  Iterator<String> data = null;
		try{
    	  //System.out.println("############# In unionStreams.flatMap ");
        String s = new String(line, StandardCharsets.UTF_8);
        data  = Arrays.asList(LINE_SEPARATOR.split(s)).iterator();
        //System.out.println("############# In unionStreams.flatMap String s:"+ s);
     
        
        while(data.hasNext()) {
        	 String text = data.next();
        	// System.out.println("############# In unionStreams.flatMap String text:"+ text);
        	 JsonObject jsonObject;
			try{
	        	
                 JsonReader reader = new JsonReader(new StringReader(text));
                 reader.setLenient(true);
                 jsonObject = new JsonParser().parse(reader).getAsJsonObject();
                 System.out.println("############# In unionStreams.flatMap String jsonObject:"+ jsonObject);
                 
                 System.out.println("############# JSON userid: "+jsonObject.get("userid").getAsString());
                 
                 
             }catch(Exception e2) {
            	 e2.printStackTrace();
             }
        	 
			
        	
        }
        }catch(Exception e) {
        	e.printStackTrace();
        }
        
        return data;
        
      }
    });*/

   

    // Print the first 10 wordCounts
   // records.print();

    // Start the streaming context and await termination
    jssc.start();
    jssc.awaitTermination();
  }


private static StructType rowSchema;
  
  
  private static StructType createSchema() {
		List<StructField> mapFields = new ArrayList<StructField>();
		for ( RowFieldsEnum fieldName: RowFieldsEnum.values()) {
		  mapFields.add(DataTypes.createStructField(fieldName.getName(), fieldName.getDataType(), true));
		}
		rowSchema = DataTypes.createStructType(mapFields);
		return rowSchema;
  }
  
}
