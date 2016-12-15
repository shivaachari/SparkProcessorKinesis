package com.hotstar.datamodel.streaming.spark.processor;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

public interface IStreamingProcessor<T> extends Serializable{
	
	
	//void initializeConfig();
	//DataModels initializeReceivers();
	/*JavaStreamingContext initializeJavaStreamingContext();
	IStreamingProcessor<T> getStreamingProcessor();
	JavaRDD<T> getDatafromStream(DataModels dataModels);
	*/
	public void processData(JavaRDD<T> data);
	public JavaRDD<T> getData();
	
}
