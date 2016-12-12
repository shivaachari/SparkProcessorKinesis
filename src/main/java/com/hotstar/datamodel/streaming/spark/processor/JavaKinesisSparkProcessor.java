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


import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.hotstar.datamodel.streaming.kinesis.util.KinesisUtil;
import com.hotstar.datamodel.streaming.spark.struct.RowFieldsEnum;
import com.hotstar.datamodel.streaming.spark.util.Constants;
import com.hotstar.datamodel.streaming.spark.util.JSONUtil;
import com.hotstar.datamodel.streaming.spark.util.SparkObjects;


public final class JavaKinesisSparkProcessor { // needs to be public for access from run-example
 // private static final Pattern WORD_SEPARATOR = Pattern.compile(" ");
	
	

public static void main(String[] args) throws Exception {
    
    String[] args1 = {"spark_processor", "ClientData_json_streams", 
            "kinesis.eu-west-1.amazonaws.com"};

    // Populate the appropriate variables from the given args
    String kinesisAppName = args1[0];
    String streamName = args1[1];
    String endpointUrl = args1[2];
    String appName = "JavaKinesisSparkProcessor";
    String master = "local[*]";
    Duration batchInterval = new Duration(10*1000); // in milliseconds
   // FieldsEnum fieldsEnum = RowFieldsEnum;
    //FieldsEnum[] fields = RowFieldsEnum.values();
    
    //Initialize Spark Config and Session
    SparkObjects.initialize(appName, master);

   
    JavaDStream<byte[]> unionStreams = KinesisUtil.readDataFromKinesis(kinesisAppName, streamName, endpointUrl, SparkObjects.getJavaStreamingContext(batchInterval));
    
    JavaDStream<Row> rowData = unionStreams.flatMap(new FlatMapFunction<byte[],Row>()
	    {
	        private static final long serialVersionUID = -2381522520231963249L;

	        @Override
	        public Iterator<Row> call(byte[] line) {
	        	return JSONUtil.parseJsonData(line);
	        }
	    });
    
    rowData.foreachRDD(new VoidFunction<JavaRDD<Row>>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 6546262932309436881L;
		private SparkSession sparkSession = SparkObjects.getSparkSession(Constants.APP_NAME);

		@Override
		public void call(JavaRDD<Row> rowRDD) throws Exception {
			
			try{
				
				SQLContext sqlContext = sparkSession.sqlContext();
				Dataset<Row> dataframe = sqlContext.createDataFrame(rowRDD, RowFieldsEnum.createSchema());
				dataframe.createOrReplaceTempView("tempHiveTable");
				sparkSession.sql("select * from tempHiveTable").show();
				
			   }catch(Exception e) {
		        	e.printStackTrace();
		        }
		}
    });
    
    try {
	     JavaStreamingContext jssc = SparkObjects.getJavaStreamingContext(batchInterval);
	    // Start the streaming context and await termination
	    jssc.start();
	    jssc.awaitTermination();
    }catch(Exception e) {
    	e.printStackTrace();
    }
    
  }

  
  
  
  
}
