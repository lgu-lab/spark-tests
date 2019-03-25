package spark.dataset;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDataset03 {
	
	public static void flushOutput() {
		System.err.flush();
		System.out.flush();
	}
	
	public static void main(String[] args) {

		System.out.println("Starting...");
		
    	SparkSession sparkSession = SparkSession.builder()
    			.appName("SparkTests")
    			.master("local[4]") // 4 partitions --> 4 tasks => 41 sec
    			.getOrCreate();
    	
		System.out.println("SparkSession ready.");
    	
    	String csv_file = "D:/TMP/csv-files/person.csv";
    	
		
		System.out.println("Creating DataFrameReader...");
    	DataFrameReader dfr = sparkSession.read()
	    	.option("header",    "true")
	    	.option("delimiter", ";")
	    	.format("csv");
		System.out.println("DataFrameReader ready.");    	
		flushOutput();
		
		System.out.println("Loading file...");
    	Dataset<Row> ds0 = dfr.load(csv_file);    	
		System.out.println("File loaded."); // Not realy loaded
		flushOutput();

//		// TRANSFORMATION : map to PERSON 
//		Dataset<Person> dataset = ds0.map((MapFunction<Row, Person>)row -> { 
//			//String s = row.<String>getAs("Id");
//			int id = Integer.parseInt( row.<String>getAs("Id").trim() );
//			String firstName = row.<String>getAs("FirstName").trim() ;
//			String lastName = row.<String>getAs("LastName").trim() ;
//			return new Person(id, firstName, lastName) ;
//			}, Encoders.bean(Person.class) );
		
		MapFunction<Row, Person> mapFunction = new PersonMapping();
    	PersonProcessing processingFunction = new PersonProcessing();
		
		
		// TRANSFORMATION : map to PERSON 
		Dataset<Person> dataset = ds0.map(mapFunction, Encoders.bean(Person.class) );
		
		// ACTION : foreach
    	System.out.println("foreach : starting tasks... " ); 	
		flushOutput();
    	dataset.foreach(processingFunction);
    	System.out.println("foreach : done. " ); 	
		flushOutput();
	}
}
