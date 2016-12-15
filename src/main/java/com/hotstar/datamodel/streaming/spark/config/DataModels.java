package com.hotstar.datamodel.streaming.spark.config;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.StringUtils;

@XmlRootElement (name = "datamodels")
public class DataModels {
	@Override
	public String toString() {
		return "DataModels [models="  + StringUtils.join(models.toArray(), "\n")+ "]";
	}

	private List<DataModel> models;
	
	@XmlElement (name = "datamodel")
	public List<DataModel> getModels() {
		return models;
	}
	
	public void setModels(List<DataModel> models) {
		this.models = models;
	}
}
