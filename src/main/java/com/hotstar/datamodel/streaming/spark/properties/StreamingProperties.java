package com.hotstar.datamodel.streaming.spark.properties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import com.hotstar.datamodel.streaming.spark.util.Constants;




public enum StreamingProperties {

	  PROTEUM_DISCOVERY_ZNODE("PROTEUM_DISCOVERY_ZNODE", "/proteum_driver_hostname"),
	  STREAMING_DISCOVERY_ZNODE("STREAMING_DISCOVERY_ZNODE", "/streaming_driver_hostname"),
	  WINDOW_DURATION("WINDOW_DURATION","10"),
	  /*PROTEUM_HOST_NAME("PROTEUM_HOST_NAME",""),
	  RECEIVER_CONFIG_PATH("RECEIVER_CONFIG_PATH", Constants.STREAMING_DEFAULT_CONF_DIR + "/config.xml"),
	  DATAMODEL_QUEUE_SIZE("DATAMODEL_QUEUE_SIZE","3"),
	  USE_TACHYON("USE_TACHYON","false"),
	  DATAMODEL_BASE_PATH("DATAMODEL_BASE_PATH","streamingdata"),
	  STREAMING_PORT("STREAMING_PORT","8350"),
	  STREAMING_BATCH_SIZE("STREAMING_BATCH_SIZE","5000"),
	  PROTEUM_PORT("PROTEUM_PORT","8359");*/
;
	
	  private String key;
	  private String value;

	  static {
		  Properties prop = new Properties();

		  try{
			  BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(new File(
							Constants.STREAMING_DEFAULT_CONF_DIR
									+ "/streaming.properties"))));
			  prop.load(reader);
		  }
		  catch(Exception e){
			  throw new RuntimeException(e);
		  }


		  for (StreamingProperties property : StreamingProperties.values()) {
			  if (prop.containsKey(property.key)) {
				  property.value = prop.getProperty(property.key);
			  }
		  }
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
