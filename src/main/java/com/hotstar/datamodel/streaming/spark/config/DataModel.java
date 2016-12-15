package com.hotstar.datamodel.streaming.spark.config;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;

import org.apache.commons.lang.StringUtils;

public class DataModel {
	private List<Receiver> receiver;
	private String name;
	private String processor;
	public DataModel(){}
	
	@XmlElement
	public List<Receiver> getReceiver() {
		return receiver;
	}
	
	public void setReceiver(List<Receiver> receiver) {
		this.receiver = receiver;
	}
	
	@XmlElement
	public String getName() {
		return name;
	}
	
	@Override
	public String toString() {
		return "DataModel [receiver=" + StringUtils.join(receiver.toArray(), ",") + ", name=" + name + ", processor=" + processor + "]";
	}

	public void setName(String name) {
		this.name = name;
	}
	
	@XmlElement
	public String getProcessor() {
		return processor;
	}
	
	public void setProcessor(String processor) {
		this.processor = processor;
	}
}
