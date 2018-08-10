package com.training.spark;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.COL;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.training.spark.commons.Connections;
import static org.apache.spark.sql.functions.*;

public class SparkSqlDemo {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		JavaSparkContext sparkContext = Connections.getContext();
		
		SparkSession sparkSession= Connections.getSession();
		
		Dataset<Row> empDataFields=sparkSession.read().json("./data/customer.json");
		
		empDataFields.show();
		empDataFields.printSchema();
		
		System.out.println("S===============>>>> elect Demo    ");
		
		empDataFields.select(col("name"),col("salary")).show();
		
		

		
		System.out.println("Male");
		empDataFields.filter(col("gender").equalTo("male")).show();
		
		
		System.out.println("================= Aggregate =================");
		empDataFields.groupBy(col("gender")).count().show();
		
//    group by id,avarage salary max age
		
		Dataset<Row> summaryDAta= 
				empDataFields.groupBy(col("deptid"))
				.agg(avg(empDataFields.col("salary")), 
						max(empDataFields.col("age")));
		summaryDAta.show();
		
		
		
		
		
		
		
	}

}
