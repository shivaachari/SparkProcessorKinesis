package com.hotstar.datamodel.streaming.kinesis.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

public class KinesisUtil {
	
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
	 * @param jssc 
	 */

	public static JavaDStream<byte[]> readDataFromKinesis(String kinesisAppName, String streamName, String endpointUrl, JavaStreamingContext jssc) {
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
	    
	    return unionStreams;
	}
}
