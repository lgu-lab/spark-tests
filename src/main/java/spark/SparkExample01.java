package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;

import scala.collection.Seq;

public class SparkExample01 {

	public static void main(String[] args) {

		System.out.println("Starting...");
		
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");

//		JavaSparkContext context = new JavaSparkContext(sparkConf);
		
		SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);
//		SparkContext sparkContext = SparkContext.getOrCreate(); //  Error initializing SparkContext : A master URL must be set in your configuration
		System.out.println("SparkContext ready.");
		System.out.println("sparkContext.version()       = " + sparkContext.version() + " ( Spark version )" );
		System.out.println("sparkContext.appName()       = " + sparkContext.appName() );
		System.out.println("sparkContext.master()        = " + sparkContext.master() );
		System.out.println("sparkContext.applicationId() = " + sparkContext.applicationId() );
		
		System.out.println("Retrieving SparkEnv...");
		SparkEnv sparkEnv = sparkContext.env() ;
		System.out.println("sparkEnv.executorId() = " + sparkEnv.executorId() );
		
		Seq<String> jars = sparkContext.jars(); // How to iterate a Scala Seq with Java ?
		// Iterator<String> iter = jars.iterator(); // ERROR
		
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

		System.out.println("JavaSparkContext ready.");
		System.out.println("javaSparkContext.version()       = " + javaSparkContext.version() + " ( Spark version )" );
		System.out.println("javaSparkContext.appName()       = " + javaSparkContext.appName() );
		System.out.println("javaSparkContext.master()        = " + javaSparkContext.master() );
		//System.out.println("javaSparkContext.applicationId() = " + javaSparkContext.applicationId() );

		// ...

		System.out.println("Closing...");
		javaSparkContext.close();
	}
}
