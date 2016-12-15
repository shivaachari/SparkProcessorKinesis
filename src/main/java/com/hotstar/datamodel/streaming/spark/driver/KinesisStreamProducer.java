package com.hotstar.datamodel.streaming.spark.driver;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

public class KinesisStreamProducer {
	
	 static Logger log = LogManager.getLogger("KinesisStreamProducer");

	public static void main(String[] args) {
		
		 
		 String stream = "ClientData_json_streams";
		 String endpoint = "kinesis.eu-west-1.amazonaws.com";
		 String recordsPerSecond = "1000";
		 String wordsPerRecord = "10";
				 
			    // Generate the records and return the totals
		generate(stream, endpoint, Integer.parseInt(recordsPerSecond), Integer.parseInt(wordsPerRecord));
			        
			    log.warn("Totals for the words sent");
			    
	}
	
	public static void generate(String stream,
			String endpoint,
			int recordsPerSecond,
			int wordsPerRecord)  {

		    
		    
		    // Create the low-level Kinesis Client from the AWS Java SDK.
		     AmazonKinesisClient kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
		    kinesisClient.setEndpoint(endpoint);

		    log.warn("Putting records onto stream $stream and endpoint $endpoint at a rate of" +
		        " $recordsPerSecond records per second and $wordsPerRecord words per record");

		  
		    //val source = scala.io.Source.fromFile("/Users/shivaa/spark/user_50.txt")
		    try {
		        // val data = source.getLines mkString "\n"
		         
		          Random r = new Random();
		         
		         // Create a partitionKey based on recordNum
		         String partitionKey = "partitionKey-"+ r.nextInt(10);
		        
		        String data = "{\"userid\":4951,\"username\":\"TNH86BKY\",\"firstname\":\"Yoshi\",\"lastname\":\"Mcconnell\",\"city\":\"Vail\",\"state\":\"DC\",\"email\":\"eu.tempor@eueleifend.edu\",\"phone\":\"(339) 447-8200\",\"likesports\":false,\"liketheatre\":true,\"likeconcerts\":true,\"likejazz\":true,\"likeclassical\":false,\"likeopera\":true,\"likerock\":false,\"likevegas\":false,\"likebroadway\":false,\"likemusicals\":true}" +
		   System.lineSeparator() +"{\"userid\":4952,\"username\":\"TVA62LLB\",\"firstname\":\"Nathan\",\"lastname\":\"Knapp\",\"city\":\"Bell\",\"state\":\"TN\",\"email\":\"enim@aliquetlobortis.edu\",\"phone\":\"(527) 178-8834\",\"likesports\":false,\"liketheatre\":false,\"likeconcerts\":false,\"likejazz\":true,\"likeclassical\":false,\"likeopera\":false,\"likerock\":false,\"likevegas\":false,\"likebroadway\":false,\"likemusicals\":false}" +
		  System.lineSeparator() +"{\"userid\":4953,\"username\":\"EKJ94CVH\",\"firstname\":\"Wang\",\"lastname\":\"Clements\",\"city\":\"Plantation\",\"state\":\"GA\",\"email\":\"molestie.orci.tincidunt@primisinfaucibus.ca\",\"phone\":\"(980) 743-3973\",\"likesports\":true,\"liketheatre\":false,\"likeconcerts\":false,\"likejazz\":false,\"likeclassical\":false,\"likeopera\":false,\"likerock\":true,\"likevegas\":true,\"likebroadway\":false,\"likemusicals\":false}";
		       
		   
		   String data1 = "{\"os\":{\"osName\":\"android\",\"osVersion\":\"4.4.2\"},\"app\":{\"appName\":\"hotstar\",\"appVersion\":\"5.9.0\"},\"player\":{\"playerName\":\"android_puppet\",\"playerVersion\":\"2,4,0,61610\"},\"content\":{\"contentId\":\"1000113232\",\"name\":\"Milon Tithi-The Mallicks Celebrate Diwali-302\",\"url\":\"https://staragvod3-vh.akamaihd.net/i/videos/jalsha/milantithi/302/1000113232_,16,54,106,180,400,800,1300,2000,_STAR.mp4.csmil/master.m3u8?hdnea=st=1478596548~exp=1478597148~acl=/*~hmac=fd25d9769d33ff6527984831e1f57accac1223878f2db9716e9257e369800c01\",\"mode\":\"vod\",\"pageRef\":\"hot.tvshows.Milon Tithi.season8.The Mallicks Celebrate Diwali.episodedetail\",\"clipBitrate\":\"0\"},\"events\":[{\"eventType\":\"playerEvent\",\"event\":{\"timestamp\":\"1478597316896\",\"eventCounter\":\"21\",\"playerEvent\":\"end\",\"position\":\"854052\",\"videoSegment\":\"1\",\"bufferingTime\":\"9771\",\"videoUrl\":\"https://staragvod3-vh.akamaihd.net/i/videos/jalsha/milantithi/302/1000113232_,16,54,106,180,400,800,1300,2000,_STAR.mp4.csmil/master.m3u8?hdnea=st=1478596548~exp=1478597148~acl=/*~hmac=fd25d9769d33ff6527984831e1f57accac1223878f2db9716e9257e369800c01\",\"contentId\":\"1000113232\",\"programTitle\":\"Milon Tithi\",\"episodeTitle\":\"The Mallicks Celebrate Diwali\"}}],\"header\":{\"userAgent\":\"PostmanRuntime/3.0.1\",\"ipAddress\":\"::ffff:220.227.50.9\",\"referer\":\"deepak2\"}}";
		  
		   Path path = Paths.get("/Users/shivaa/git/SparkProcessorKinesis_git/src/main/resources/datamodel/streaming/inputs/clientevent.json");
		   data = new String(Files.readAllBytes(path));
		   
		       // Create a PutRecordRequest with an Array[Byte] version of the data
		          PutRecordRequest putRecordRequest = new PutRecordRequest().withStreamName(stream)
		            .withPartitionKey(partitionKey)
		            .withData(ByteBuffer.wrap(Files.readAllBytes(path)));
		            //.withData(ByteBuffer.wrap(data1.getBytes()))

		        // Put the record onto the stream and capture the PutRecordResult
		         kinesisClient.putRecord(putRecordRequest);
		        
		        // Sleep for a second
		      
		       log.warn("Sent "+Files.lines(path).count()+" records");
		       System.out.println("Sent "+Files.lines(path).count()+" records");
		    }
		    catch (Exception e){
		       log.error("exception caught: ",e);
		    }
	}

}
