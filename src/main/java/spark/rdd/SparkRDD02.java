package spark.rdd;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRDD02 {

	public static void main(String[] args) {

		System.out.println("Starting...");
		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");

		JavaSparkContext context = new JavaSparkContext(sparkConf);

		System.out.println("JavaSparkContext ready.");
		// ...
		
		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(1,2,3,4,5,6));
		System.out.println("----------");
		System.out.println("Initial RDD : ");
		System.out.println(numbersRDD.collect().toString());
		
		System.out.println("----------");
		System.out.println("map( n -> n*n ) ... ");
		JavaRDD<Integer> squaresRDD = numbersRDD.map( n -> n*n );
		System.out.println("collect() : ");
		System.out.println(squaresRDD.collect().toString());

		System.out.println("----------");
		System.out.println("filter( n -> n%2==0 ) ... ");
		JavaRDD<Integer> evenRDD = squaresRDD.filter( n -> n%2==0 );
		System.out.println("collect() : ");
		System.out.println(evenRDD.collect().toString());
		
		System.out.println("----------");
		System.out.println("flatMap( n->Arrays.asList(n,n*2,n*3).iterator()) ... ");
		JavaRDD<Integer> multipliedRDD = numbersRDD.flatMap( n->Arrays.asList(n,n*2,n*3).iterator());
		System.out.println("collect() : ");
		System.out.println(multipliedRDD.collect().toString());
		
		System.out.println("----------");
		System.out.println("Closing...");
		context.close();
	}
}
