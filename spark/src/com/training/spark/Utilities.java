package com.training.spark;

import org.apache.spark.api.java.JavaRDD;

public class Utilities {
	
	public static void printStringRDD(JavaRDD<String> stringRdd,int limit) {
		
		for(String temp:stringRdd.take(limit)) {
			System.out.println(temp);
		}
		System.out.println("-----------------------------");
	}
	
	
	public static void hold() {
		while (true) {

			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}


	public static void printDis(JavaRDD<Integer> colldata) {

		colldata.distinct().collect().forEach(System.out::println);

	}

}
