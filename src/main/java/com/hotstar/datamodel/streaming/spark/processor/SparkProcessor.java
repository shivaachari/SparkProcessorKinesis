package com.hotstar.datamodel.streaming.spark.processor;

import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.hotstar.datamodel.streaming.spark.struct.RowFieldsEnum;
import com.hotstar.datamodel.streaming.spark.util.JSONUtil;
import com.hotstar.datamodel.streaming.spark.util.SparkObjects;

public class SparkProcessor implements IStreamingProcessor<byte[]> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3779192400421621154L;

	@Override
	public void processData(JavaRDD<byte[]> data) {
		
		try{
		 SparkSession sparkSession = SparkObjects.getSparkSession();
		
		  JavaRDD<Row> rowDataRDD = data.flatMap(new FlatMapFunction<byte[],Row>()
		    {
		        private static final long serialVersionUID = -2381522520231963249L;

		        @Override
		        public Iterator<Row> call(byte[] line) {
		        	return JSONUtil.parseJsonData(line);
		        }
		    });
		  
			
				
				SQLContext sqlContext = sparkSession.sqlContext();
				Dataset<Row> dataframe = sqlContext.createDataFrame(rowDataRDD, RowFieldsEnum.createSchema());
				dataframe.printSchema();
				dataframe.createOrReplaceTempView("tempHiveTable");
				
				//sparkSession.sql("describe tempHiveTable").show();
				sparkSession.sql("select * from tempHiveTable").show();
				//sparkSession.sql("select  from tempHiveTable group by platform").show();
				
			   } catch (Exception e) {
		        	e.printStackTrace();
		        }
			
		
	}

	@Override
	public JavaRDD<byte[]> getData() {
		// TODO Auto-generated method stub
		return null;
	}
	
	

	

}
