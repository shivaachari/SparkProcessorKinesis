package com.hotstar.datamodel.streaming.spark.util;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class JSONUtil {

	 public  static final Pattern LINE_SEPARATOR = Pattern.compile(System.lineSeparator());
	 public  static final Pattern DOT_SEPARATOR = Pattern.compile("\\.");
	
    public static Iterator<Row> parseJsonData(byte[] data) {
		ArrayList<Row> rowData = new ArrayList<Row>();
		try{
			
			String s = new String(data, StandardCharsets.UTF_8);
			
		
			ParseStringJSON(s, rowData);
			
         //rowArray.add(rowData);
     }catch(Exception e2) {
    	 e2.printStackTrace();
     }
		
		return  rowData.iterator();
	}
    
    public static void ParseStringJSON(String s, ArrayList<Row> rowData ) {
    	
    	System.out.println("input is : " + s);
    	final Pattern LINE_SEPARATOR = Pattern.compile(System.lineSeparator());
    	String[] lines = LINE_SEPARATOR.split(s);
    	for(String line : lines) {
			 JsonReader reader = new JsonReader(new StringReader(line));
            reader.setLenient(true);
            JsonObject jsonObject = new JsonParser().parse(reader).getAsJsonObject();
          
          try{
            rowData.add( RowFactory.create( String.valueOf(System.currentTimeMillis()),
            		
            		getValueFromJSON(jsonObject, "os.osVersion"),
            		getValueFromJSON(jsonObject, "app.appVersion"),
            		getValueFromJSON(jsonObject, "content.name"),
            		getValueFromJSON(jsonObject, "events[0].event.playerEvent"),
            		getValueFromJSON(jsonObject, "content.pageRef")
            		
            		)
           		 );
          }
          catch(Exception ex) {
        	  System.out.println(ex.getMessage());
          }
		}
    }
    
    static String getValueFromJSON(JsonObject jsonObject, String keyString) {
    	try{
    	
	    	JsonObject jsonObject1 = jsonObject;
	    	String[] keys = DOT_SEPARATOR.split(keyString);
	    	for(int i=0; i< keys.length-1; i++) {
	    		jsonObject1 = jsonObject1.getAsJsonObject(keys[i]);
	    	}
	    	
	    	return jsonObject1.get(keys[keys.length-1]).getAsString();
    	}
    	catch(Exception ex) {
    		System.out.println(ex.getMessage());
    		return "";
    	}
    	
    }

}
