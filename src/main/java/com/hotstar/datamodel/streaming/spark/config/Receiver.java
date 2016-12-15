package com.hotstar.datamodel.streaming.spark.config;

import javax.xml.bind.annotation.XmlElement;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;


public class Receiver {
	private int num_threads;
	private String kinesisAppName;
	private String streamName;
	private String endpointUrl;
	private String initialPositionInStream ="LATEST";
	private String storageLevel="MEMORY_AND_DISK";
	private int duration=10;
	
	@XmlElement
	public String getKinesisAppName() {
		return kinesisAppName;
	}

	@XmlElement
	public int getDuration() {
		return duration;
	}
	public Duration getDurationObject() {
		return new Duration(duration*1000);
	}
	
	public void setDuration(int duration) {
		System.out.println("######## Setting setDuration in Receiver : "+ duration);
		//this.duration = new Duration(Integer.parseInt(duration) * 1000);
		this.duration = duration;
	}

	

	public void setKinesisAppName(String kinesisAppName) {
		this.kinesisAppName = kinesisAppName;
	}

	@XmlElement
	public String getStreamName() {
		return streamName;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	@XmlElement
	public String getEndpointUrl() {
		return endpointUrl;
	}

	public void setEndpointUrl(String endpointUrl) {
		this.endpointUrl = endpointUrl;
	}

	@XmlElement
	public String getInitialPositionInStream() {
		return initialPositionInStream;
	}
	
	public InitialPositionInStream getInitialPositionInStreamObject() throws Exception {
		switch (initialPositionInStream) {
		  case "LATEST" :
			  return InitialPositionInStream.LATEST;
			 
		  case "TRIM_HORIZON" :
			  return InitialPositionInStream.TRIM_HORIZON;
			  
		  default:
			  throw new Exception("Incorrect StorageLevel");
		  }
	}

	public void setInitialPositionInStream(String initialPositionInStream) throws Exception {
		System.out.println("######## Setting setInitialPositionInStream in Receiver : "+ initialPositionInStream);
		 this.initialPositionInStream = initialPositionInStream;
	}

	@XmlElement
	public String getStorageLevel() {
		return storageLevel;
	}
	public StorageLevel getStorageLevelObject() throws Exception {
		switch (storageLevel) {
		  case "MEMORY_AND_DISK" :
			  return StorageLevel.MEMORY_AND_DISK();
			  
		  case "MEMORY_AND_DISK_2" :
			  return StorageLevel.MEMORY_AND_DISK_2();
			  
			  
		  case "MEMORY_AND_DISK_SER" :
			  return StorageLevel.MEMORY_AND_DISK_SER();
			  
		  case "MEMORY_AND_DISK_SER_2" :
			  return StorageLevel.MEMORY_AND_DISK_SER_2();
			  
			  
		  case "MEMORY_ONLY" :
			  return StorageLevel.MEMORY_ONLY();
			  
		  case "MEMORY_ONLY_2" :
			  return StorageLevel.MEMORY_ONLY_2();
			  
			  
		  case "MEMORY_ONLY_SER" :
			  return StorageLevel.MEMORY_ONLY_SER();
			  
		  case "MEMORY_ONLY_SER_2" :
			  return StorageLevel.MEMORY_ONLY_SER_2();
			  
			  
			  default:
				  throw new Exception("Incorrect StorageLevel");
		  }
	}

	public void setStorageLevel(String storageLevel) throws Exception {
		System.out.println("######## Setting setstorageLevel in Receiver : "+ storageLevel);
		this.storageLevel=storageLevel;
	}

	public Receiver(){}
	
	@XmlElement
	public int getNum_threads() {
		return num_threads;
	}
	public void setNum_threads(int num_threads) {
		this.num_threads = num_threads;
	}

	@Override
	public String toString() {
		return "Receiver [num_threads=" + num_threads + ", kinesisAppName=" + kinesisAppName + ", streamName="
				+ streamName + ", endpointUrl=" + endpointUrl + ", initialPositionInStream=" + initialPositionInStream.toString()
				+ ", storageLevel=" + storageLevel.toString() + ", duration=" + duration + "]";
	}
	
}
