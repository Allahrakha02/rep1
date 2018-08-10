package com.training.spark;

import java.util.Arrays;

import org.apache.spark.api.java.function.Function;

public class MPGWorks implements Function<String, Double> {
	
	int totalMPGCity;
	int totalMPGHwy;

	
	
	
	@Override
	public Double call(String v1) throws Exception {
		String[] attributes = v1.split(",");
		
		totalMPGCity=Integer.parseInt(attributes[9]);
		
		totalMPGHwy=Integer.parseInt(attributes[10]);
		
		return 12.0;
	}

public double getAvaregeMPGCity(int count) {
		
		return totalMPGCity/count;
	}
public double getAvaregeMPGHWY(int count) {
	
	return totalMPGHwy/count;
}
	
}
