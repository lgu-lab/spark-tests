package spark.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDataset01 {

	public static void main(String[] args) {

		System.out.println("Starting...");
		
    	SparkSession sparkSession = SparkSession.builder()
    			.appName("SparkTests")
    			.master("local[4]")
    			.getOrCreate();
    	
		System.out.println("SparkSession ready.");
    	
    	String csv_file = "C:\\Users\\ybenabde\\Downloads\\csv\\large_file.csv";
    	
		System.out.println("Loading file...");
    	Dataset<Row> df = sparkSession.read()
    			.option("header", "true")
    			.option("delimiter", ",")
    			.format("csv")
    			.load(csv_file);
    	
		System.out.println("File loaded.");
		
    	System.out.println("Dataset count  : " + df.count() );
    	System.out.println("Dataset schema : " + df.schema() );
	}
	
}
