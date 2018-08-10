package com.training.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.activation.DataSource;

import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.StatCounter;

import com.training.spark.commons.Connections;
import static org.apache.spark.sql.functions.*;

public class SparkOperationClient {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		JavaSparkContext sparkContext = Connections.getContext();

		// Start loading data
		// load the collection and cache it

		JavaRDD<Integer> colldata = DAtaSources.getCollData();

		System.out.println("CollDAts :" + colldata.count());
		//
		// load the file and cache

		JavaRDD<String> autoDataContent = sparkContext.textFile("./data/auto-data.csv");

		System.out.println("Auto data count :" + autoDataContent.count());

		autoDataContent.take(10).forEach(System.out::println);

		System.out.println("Loading Data From File \n\n");

		// Utilities.printStringRDD(autoDataContent, 10);

		// Storing RDD's

		// autoDataContent.saveAsTextFile("./data/auto-data-response.csv");

		JavaRDD<String> tsvData = autoDataContent.map(str -> str.replace(",", "\t"));

		System.out.println("Tsv Format");
		System.out.println("----------------------------");
		Utilities.printStringRDD(tsvData, 5);

		// Filter Example

		 String geader=autoDataContent.first();
		
		 JavaRDD<String> autoDataWriteWithoutHeader=
		 autoDataContent.filter(s->!s.equals(geader));
		System.out.println("\n ------------------- \n\n");
		 Utilities.printStringRDD(autoDataWriteWithoutHeader, 5);

		System.out.println("=================== Toyata Data ================");
		JavaRDD<String> toyataData = autoDataContent.filter(str -> str.contains("toyota"));
		Utilities.printStringRDD(toyataData, 10);

		System.out.println("=================== unique Data ==================");
		JavaRDD<String> uniqueList = autoDataContent.distinct();
		// Utilities.printStringRDD(uniqueList, 10);

		JavaRDD<String> uList = autoDataContent.distinct(20);

		System.out.println("<================== New Distinct Record ================>");

		Utilities.printDis(colldata);

		// to count no of words

		System.out.println("Using Flat Map ");

		JavaRDD<String> words = autoDataContent.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(t.split(",")).iterator();
			}

		});
		System.out.println(words.count());
		
		//after cleansing the data
		
		System.out.println("******************** After cleansing of data **************************");
		
		JavaRDD<String> cleanseRDD= autoDataContent.map(new ClenseRDDCar());
		
		Utilities.printStringRDD(cleanseRDD, 5);
		
		
		
		
		//Set Operations
		JavaRDD<String> word1=sparkContext.parallelize(Arrays.asList("hello","how","are","you","today"));
		JavaRDD<String> word2=sparkContext.parallelize(Arrays.asList("hello","how","were","yesterday"));
		
		
		System.out.println("Union Operation s -set");
		
		Utilities.printStringRDD(word1.union(word2), 9);
		
		
		
System.out.println("Intersection Operation s -set");
		
		Utilities.printStringRDD(word1.intersection(word2), 9);
		
		Integer autoDataContentDataCount=colldata.reduce((x,y)->x+y);
	
		//JavaRDD<Integer> colldataCount=colldata.reduce((x,y)->x+y);
		
		System.out.println("Auto Count :  "+autoDataContentDataCount);
		
		//Avarage milese in MPG-HWY
		//System.out.println(colldata.toString());
		
		//[MAKE, FUELTYPE, ASPIRE, 4, BODY, DRIVE, CYLINDERS, HP, RPM, MPG-CITY, MPG-HWY, PRICE]
		
		
//		long d=autoDataContent.mapToDouble(x->Double.parseDouble(x)).stats().count();
//		System.out.println("===============>>>  "+d);
		
		
		JavaRDD<Integer> cnt= colldata.filter(str->str.equals("MPG-CITY"));
		
		
		Department dept1 = new Department(100, "Testing");
		Department dept2 = new Department(200, "Development");
		
		SparkSession session= Connections.getSession();
		
		List<Department> deptList= new ArrayList<>();
		deptList.add(dept1);
		deptList.add(dept2);
		
		Dataset<Row> deptDatafields= session.createDataFrame(deptList, Department.class);
		deptDatafields.show();
		
		
		
		Dataset<Row> empDataFields=session.read().json("./data/customer.json");
		
		
	//	Dataset<Row>  joinField=empDataFields.join(dr,col("id").equalTo("deptId"));
		
		empDataFields.filter(col("age").gt(30))
		.join(deptDatafields,col("deptid").equalTo("departmentId")).
		groupBy(col("deptid")).agg(
				avg(empDataFields.col("salary")),
				max(empDataFields.col("age"))
				).show();
		
		
		
		System.out.println("<<<< +9=================================== Auto data ==================================== >>>>");
		
		Dataset<Row> autoData=session
				.read().option("header", "true")
				.csv("./data/auto-data.csv");
		autoData.show(5);
		
		
		
		
		//Integer avarege= autoDataWriteWithoutHeader.mapToDouble(str->str.valueOf(""))
		
		
		Row row1=RowFactory.create(1,"India","Bengaluru");
		Row row2=RowFactory.create(2,"Usa","Reston");
		Row row3=RowFactory.create(3,"UK","Steevencreek");
		
		List<Row> rlist=new ArrayList<Row>();
		rlist.add(row1);
		rlist.add(row2);
		rlist.add(row3);
		JavaRDD<Row> rowRdd=sparkContext.parallelize(rlist);
		
		
		
		System.out.println("<<<<<<<<<<< dataset  >>>>>>>>>>>>");
		System.out.println("     <<<<<<<<<  >>>>>>>>");
		
		StructType schema= DataTypes.createStructType(new StructField[] {

				DataTypes.createStructField("id", 
						DataTypes.IntegerType, false),
				DataTypes.createStructField("country", 
						DataTypes.StringType, false),
				DataTypes.createStructField("city", 
						DataTypes.StringType, false)
		});
		
		
		Dataset<Row> tempDataFields= session.createDataFrame(rowRdd, schema);
		tempDataFields.show();
		
		
		autoData.createOrReplaceTempView("autos");
		
		System.out.println("Temp Table Contents ");
		
		System.out.println("Select all fields greater than 200+");
		
		session.sql("select * from autos where hp>200").show();
		
		//to find make, maximum
		
		System.out.println("rpm");
		
		session.sql("select make, max(rpm) from autos group by make order by 2").show();
		
		JavaRDD<Row> autoRDD=autoData.rdd().toJavaRDD();
		
		// reading data from mySQl DB
		
		
		//db (exdb) , table(employee)
		
		
		
		Map<String, String> jdbcConnectParams= new HashMap<String, String>();
		
		jdbcConnectParams.put("url", "jdbc:mysql://localhost:3306/airlineDb");
		jdbcConnectParams.put("driver", "com.mysql.jdbc.Driver");
		jdbcConnectParams.put("dbtable", "employee");
		jdbcConnectParams.put("user", "root");
		jdbcConnectParams.put("password", "root@123");
		
		
		System.out.println("Create a data from a DB table");
		
		
		Dataset<Row> sqlDataFields=session.read().format("jdbc").options(jdbcConnectParams).load();
		sqlDataFields.show();
		
		Utilities.hold();
		

	}
}
