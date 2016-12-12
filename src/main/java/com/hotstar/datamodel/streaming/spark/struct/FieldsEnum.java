package com.hotstar.datamodel.streaming.spark.struct;
import org.apache.spark.sql.types.DataType;

public interface FieldsEnum {

	int index();

	DataType getDataType();

	String getName();
	
	

}