package com.hotstar.datamodel.streaming.spark.iterator;

import java.util.Iterator;

public interface IRecordIterator extends Iterator<Object[]>{
	public void init(byte[] data);
	public void init(byte[] data, int offset, String pathString);
}
