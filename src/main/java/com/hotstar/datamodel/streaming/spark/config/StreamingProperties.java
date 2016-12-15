package com.hotstar.datamodel.streaming.spark.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import com.hotstar.datamodel.streaming.spark.util.Constants;



public enum StreamingProperties {
	
	
	APP_HOME("APP_HOME", "/opt/hotstar"),
	PLATFORM_HOME("PLATFORM_HOME",APP_HOME+"/Spark"),
	ENV("ENV","CLOUDERA"),
	APP_JARS("APP_JARS",""),
	SPARK_DRIVER_MEMORY("SPARK_DRIVER_MEMORY","3g"),
	SPARK_EXECUTOR_INSTANCES("SPARK_EXECUTOR_INSTANCES","3"),
	SPARK_EXECUTOR_MEMORY("SPARK_EXECUTOR_MEMORY","4g"),
	SPARK_EXECUTOR_CORES("SPARK_EXECUTOR_CORES","4"),
	WINDOW_DURATION("WINDOW_DURATION","10"),
	//RECEIVER_CONFIG_FILE_NAME("RECEIVER_CONFIG_FILE_NAME", "/config/config.xml"),
	STREAMING_PORT("STREAMING_PORT","8350"),
	STREAMING_BATCH_SIZE("STREAMING_BATCH_SIZE","5000"),
	APP_MONITOR_PORT("APP_MONITOR_PORT","8359"),
	
	SPARK_PROCESSOR_HOME("SPARK_PROCESSOR_HOME","/opt/hotstar/SparkProcessor"),
	SPARK_PROCESSOR_CLASS_NAME("SPARK_PROCESSOR_CLASS_NAME","/opt/hotstar/SparkProcessor"),
	APP_NAME("APP_NAME","Spark_Processor"),
	MASTER("MASTER","local[*]"),
	;
	  
	  private String key;
	  private String value;
	  static boolean isInitialized= false;
	  
	  public static DataModels initialize() throws Exception {  
		  Properties prop = new Properties();

			  BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(new File(Constants.APP_HOME_DIR +
							Constants.STREAMING_PROPERTIES_CONF_PATH))));
			  prop.load(reader);
		 


			  for (StreamingProperties property : StreamingProperties.values()) {
				  if (prop.containsKey(property.key)) {
					  property.value = prop.getProperty(property.key);
				  }
			  }
		  
			  File file = new File( Constants.APP_HOME_DIR +
						Constants.RECEIVER_RELATIVE_CONF_DIR);
				JAXBContext jaxbContext = JAXBContext
						.newInstance(DataModels.class);
				Unmarshaller jaxbUnmarshaller = jaxbContext
						.createUnmarshaller();
				DataModels models = (DataModels) jaxbUnmarshaller.unmarshal(file);
				
				System.out.println(models.toString());
			 // prop.load(reader);
				isInitialized = true;
				return models;
		  
		  
	  }

	  private StreamingProperties(String key, String defaultValue) {
	    this.key = key;
	    this.value = defaultValue;
	  }

	  public String getValue() {
	    return value;
	  }

	  public boolean getBooleanValue() {
	    return Boolean.parseBoolean(getValue());
	  }

	  public void setValue(String value) {
	    this.value = value;
	  }

	  public int getIntValue() {
	    return Integer.parseInt(getValue());
	  }

	  public long getLongValue() {
	    return Long.parseLong(getValue());
	  }

	}
