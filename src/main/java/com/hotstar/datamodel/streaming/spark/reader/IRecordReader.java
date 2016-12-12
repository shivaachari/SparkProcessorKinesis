package com.hotstar.datamodel.streaming.spark.reader;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

public interface IRecordReader {
	
	 void initialize();
	 boolean nextKeyValue() throws IOException ;
	 LongWritable getCurrentKey() throws IOException, InterruptedException;
	 void close() throws IOException;

}
