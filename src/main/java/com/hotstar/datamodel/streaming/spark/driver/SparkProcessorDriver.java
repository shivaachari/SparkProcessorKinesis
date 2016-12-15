package com.hotstar.datamodel.streaming.spark.driver;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.hotstar.datamodel.streaming.spark.config.DataModel;
import com.hotstar.datamodel.streaming.spark.config.DataModels;
import com.hotstar.datamodel.streaming.spark.config.StreamingProperties;
import com.hotstar.datamodel.streaming.spark.processor.IStreamingProcessor;
import com.hotstar.datamodel.streaming.spark.util.Constants;
import com.hotstar.datamodel.streaming.spark.util.KinesisUtil;
import com.hotstar.datamodel.streaming.spark.util.SparkObjects;


public class SparkProcessorDriver {
	

	public static void main(String[] args) {
		
		String home = System.getenv("HOTSTAR_HOME");
		if(home == null || "".equals(home)) { 
			System.out.println("ERROR: HOTSTAR_HOME is not set in envoironment");
			System.exit(0);
		}
		DataModels dataModels = null;
		Constants.setHOTSTAR_HOME(home);
		try{
			dataModels = initializeConfig();
		}
		catch(Exception e) {
			System.err.println("Error while initializing the environment");
			e.printStackTrace();
			System.exit(-1);
		}
		try{
			processing(dataModels);
		}
		catch(Exception e) {
			System.err.println("Error while processing the job");
			e.printStackTrace();
			System.exit(-1);
		}
		
	}

	private static void processing(DataModels models) {
		 try {
		
		JavaStreamingContext jssc = SparkObjects.getJavaStreamingContext(
				new Duration(StreamingProperties.WINDOW_DURATION.getIntValue() * 1000));
		for (DataModel model : models.getModels()) {
			
			System.out.println("##########" +model.toString());
			System.out.println("##########" +model.getReceiver().toString());
			JavaDStream<byte[]> stream = 
							KinesisUtil.readDataFromKinesis(jssc, 
									model.getReceiver());
			stream.persist(StorageLevel.MEMORY_ONLY_SER_2());
			System.out.println("################ Data is persisted in Memory count: " + stream.count());
			IStreamingProcessor<byte[]> processor = null;
			try {
				processor = (IStreamingProcessor<byte[]>) Class.forName(
						model.getProcessor()).newInstance();					
				
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			
			
			final IStreamingProcessor<byte[]> processorCopy = processor;
			
			stream.foreachRDD(new VoidFunction<JavaRDD<byte[]>>() {

				private static final long serialVersionUID = -5442367472112689457L;

				@Override
				public void call(JavaRDD<byte[]> arg0) throws Exception {
					try{
						processorCopy.processData(arg0);
					}
					catch(Exception e){
						System.err.println("Error while processing Data : ");
						e.printStackTrace();
					}
				}
			});
		}
		     //JavaStreamingContext jssc = SparkObjects.getJavaStreamingContext(batchInterval);
		    // Start the streaming context and await termination
		    jssc.start();
		    jssc.awaitTermination();
	    }catch(Exception e) {
	    	e.printStackTrace();
	    	throw new RuntimeException(e);
	    }
	}
	
	public static DataModels initializeConfig() throws Exception {
		DataModels dataModels = StreamingProperties.initialize();
		SparkObjects.initialize(StreamingProperties.APP_HOME.getValue(), 
				StreamingProperties.MASTER.getValue());
		
		return dataModels;
	}

	

}
