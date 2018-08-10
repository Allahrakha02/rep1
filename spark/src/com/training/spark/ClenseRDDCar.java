package com.training.spark;

import java.util.Arrays;

import org.apache.spark.api.java.function.Function;

public class ClenseRDDCar implements Function<String, String> {

	@Override
	public String call(String v1) throws Exception {
		String[] attributes = v1.split(",");
		// change charecters to number
		attributes[3] = (attributes[3].equals("two")) ? "2" : "4";
		attributes[4] = attributes[4].toUpperCase();

		return Arrays.toString(attributes);
	}
	
	
	
}
