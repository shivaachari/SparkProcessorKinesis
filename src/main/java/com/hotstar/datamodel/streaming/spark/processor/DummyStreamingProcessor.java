package com.hotstar.datamodel.streaming.spark.processor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class DummyStreamingProcessor implements IStreamingProcessor<Object[]> {
	JavaRDD<Object[]> stats;
	@Override
	public void processData(JavaRDD<Object[]> data) {
		if(stats!=null) stats.unpersist();
		stats = data.map(new Function<Object[], Object[]>() {

			@Override
			public Object[] call(Object[] arg0) throws Exception {
				return arg0;
			}
		});
		stats.cache();
		stats.count();
	}

	@Override
	public JavaRDD<Object[]> getData() {
		return stats;
	}

}
