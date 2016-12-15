package com.hotstar.datamodel.streaming.spark.util;

public class Constants {

	
	public static String APP_HOME_DIR;
	public static final String STREAMING_RELATIVE_CONF_DIR =  "/config/";
	public static final String RECEIVER_RELATIVE_CONF_DIR = "/config/config.xml";
	public static final String STREAMING_PROPERTIES_CONF_PATH = "/config/streaming.properties";
	//public static final String RECEIVER_CONFIG_FILE_NAME = null;
	public static String APP_NAME;
	public static String HOTSTAR_HOME;
	
	/*public static void setAPP_HOME_DIR(String APP_HOME_DIR) {
		Constants.APP_HOME_DIR = APP_HOME_DIR;
	}*/
	
	public static void setHOTSTAR_HOME(String HOTSTAR_HOME) {
		Constants.HOTSTAR_HOME = HOTSTAR_HOME;
		Constants.APP_HOME_DIR = HOTSTAR_HOME + "/spark";
	}
	
	public static void setAPP_NAME(String APP_NAME) {
		Constants.APP_NAME = APP_NAME;
	}

}
