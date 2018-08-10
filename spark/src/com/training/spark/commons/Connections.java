package com.training.spark.commons;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Connections {
	
	private static String appName="sampleApp";
	private static String sparkMaster="local[2]";
	private static JavaSparkContext spcontext=null;
	private static SparkSession sparkSession= null;
	/*
	 * 
	 * have to create this folder manually
	 * 
	 * 
	 * 
	 */
	private static String tempDirectory="/Users/allah.rakha/Desktop/wearhouse";
	
	
	public static void getConnection() {
		
		if(spcontext == null) {
			SparkConf conf= new SparkConf().setAppName(appName).setMaster(sparkMaster);
			
			spcontext= new JavaSparkContext(conf);
			
			sparkSession= SparkSession.builder()
					.appName(appName)
					.master(sparkMaster)
					.config("spark.sql.warehouse.dir", tempDirectory)
					.getOrCreate();
		}
		
	}
	public static JavaSparkContext getContext() {
		
		if(spcontext==null) {
			getConnection();
		}
		return spcontext;
	}
	
	public static  SparkSession getSession() {
		
		if(sparkSession==null) {
			getConnection();
		}
		return sparkSession;
	}
	

}
