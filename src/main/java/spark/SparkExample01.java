package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkExample01 {

	public static void main(String[] args) {

		System.out.println("Starting...");
		
		
		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");

		JavaSparkContext context = new JavaSparkContext(sparkConf);

		System.out.println("JavaSparkContext ready.");
		
		System.out.println("JavaSparkContext : Spark version / context.version() = " + context.version() );
		// ...

		System.out.println("Closing...");
		context.close();
	}
}
