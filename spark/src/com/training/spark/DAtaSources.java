package com.training.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.training.spark.commons.Connections;

public class DAtaSources {
	
	
	
	public static JavaRDD<Integer> getCollData(){
		JavaSparkContext spContext=Connections.getContext();
		
		List<Integer> data=Arrays.asList(3,4,56,43,2,66,77,23);
		
		JavaRDD<Integer> collData=spContext.parallelize(data);
		
		collData.cache();
		
		return collData;
	}

}
