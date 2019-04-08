package env;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;

public class EnvSpark {

	public static void println(String s) {
		System.out.println(s);
	}
	
	public static void main(String[] args) {

		System.out.println("Starting...");
		
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");
		System.out.println("SparkConf ready.");

		SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);
//		SparkContext sparkContext = SparkContext.getOrCreate(); //  Error initializing SparkContext : A master URL must be set in your configuration
		println("SparkContext :");
		println("sparkContext.version()       = " + sparkContext.version() + " ( Spark version )" );
		println("sparkContext.appName()       = " + sparkContext.appName() );
		println("sparkContext.master()        = " + sparkContext.master() );
		println("sparkContext.applicationId() = " + sparkContext.applicationId() );
		
		println("");
		println("SparkEnv :");
		SparkEnv sparkEnv = sparkContext.env() ;
		println("sparkEnv.executorId() = " + sparkEnv.executorId() );
		
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

		println("");
		println("JavaSparkContext :");
		println("javaSparkContext.version()       = " + javaSparkContext.version() + " ( Spark version )" );
		println("javaSparkContext.appName()       = " + javaSparkContext.appName() );
		println("javaSparkContext.master()        = " + javaSparkContext.master() );
		//println("javaSparkContext.applicationId() = " + javaSparkContext.applicationId() );

		println("");
		println("Closing...");
		javaSparkContext.close();
	}

}
