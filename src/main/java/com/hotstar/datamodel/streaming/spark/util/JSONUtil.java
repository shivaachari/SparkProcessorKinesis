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

	  private static final Pattern LINE_SEPARATOR = Pattern.compile(System.lineSeparator());
	
    public static Iterator<Row> parseJsonData(byte[] data) {
		ArrayList<Row> rowData = new ArrayList<Row>();
		try{
			
			String s = new String(data, StandardCharsets.UTF_8);
			String[] lines = LINE_SEPARATOR.split(s);
		
			
			for(String line : lines) {
				 JsonReader reader = new JsonReader(new StringReader(line));
	             reader.setLenient(true);
	             JsonObject jsonObject = new JsonParser().parse(reader).getAsJsonObject();
	             System.out.println("############# In unionStreams.flatMap String jsonObject:"+ jsonObject);
	             
	             System.out.println("############# JSON userid: "+jsonObject.get("userid").getAsString());
	             
	           
	             rowData.add( RowFactory.create(System.currentTimeMillis(),
	            		 jsonObject.get("userid").getAsString(),
	            		 jsonObject.get("firstname").getAsString(),
	            		 jsonObject.get("username").getAsString(),
	            		 jsonObject.get("email").getAsString(),
	            		 jsonObject.get("state").getAsString())
	            		 );
			}
         //rowArray.add(rowData);
     }catch(Exception e2) {
    	 e2.printStackTrace();
     }
		
		return  rowData.iterator();
	}

}
