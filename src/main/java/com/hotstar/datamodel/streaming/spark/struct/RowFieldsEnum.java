package com.hotstar.datamodel.streaming.spark.struct;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public enum RowFieldsEnum implements FieldsEnum {
	TIMESTAMP("timestamp",DataTypes.StringType),
	USERID("userid", DataTypes.StringType),
	PLATFORM("platform", DataTypes.StringType),
	CONTENT("contentid", DataTypes.StringType),
	ERROR("error", DataTypes.StringType),
	LOCATION("location", DataTypes.StringType)
	;
	
	String name;
	
	DataType dataType;
	RowFieldsEnum(String name, DataType dataType) {
		this.name = name;
		this.dataType = dataType;
	}
	
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public DataType getDataType() {
		return dataType;
	}
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}
	
	public int index() {
		return ordinal();
	}


	public static StructType createSchema() {
		List<StructField> mapFields = new ArrayList<StructField>();
		for ( RowFieldsEnum fieldName: values()) {
			mapFields.add(DataTypes.createStructField(fieldName.getName(), fieldName.getDataType(), true));
		}
		StructType rowSchema = DataTypes.createStructType(mapFields);
		return rowSchema;
	}
	
	  /*StructType createSchema() {
		List<StructField> mapFields = new ArrayList<StructField>();
		for ( RowFieldsEnum fieldName: this.values()) {
		  mapFields.add(DataTypes.createStructField(fieldName.getName(), fieldName.getDataType(), true));
		}
		StructType rowSchema = DataTypes.createStructType(mapFields);
		return rowSchema;
  }*/
	
}
