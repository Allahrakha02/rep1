package com.training.spark;

import org.apache.commons.io.input.TeeInputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadMovieTweets {
	
	public static void main(String[] args) {
		
		
		String appName="sampleApp";
		
		//as of now we have got two instances fo
		//of spark or this can be got from remote 
		//instance
		
		String sparkMaster="local[2]";
		
		JavaSparkContext sparkContext=null;
		
		SparkConf conf = new SparkConf()
				.setAppName(appName)
				.setMaster(sparkMaster);
		
		
		
		//creating saprk context from configuration
		sparkContext= new JavaSparkContext(conf);
		
		//read the file into RDD
		JavaRDD<String> tweetsRDD= sparkContext.textFile("./data/movietweets.csv");
		
		tweetsRDD.take(5).forEach(System.out::println);
		
		int count = (int)tweetsRDD.count();
		
		System.out.println(count);
		
		JavaRDD<String> upperCaseRDD=tweetsRDD.map(temp -> temp.toUpperCase());
		
		System.out.println("************************************************************");
		upperCaseRDD.take(10).forEach(System.out::println);
		
		try {
		Thread.sleep(1000);
		} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	

}
