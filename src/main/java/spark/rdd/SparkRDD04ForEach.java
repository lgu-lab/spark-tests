package spark.rdd;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRDD04ForEach {

	public static void main(String[] args) {

		System.out.println("Starting...");
		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");

		JavaSparkContext context = new JavaSparkContext(sparkConf);

		System.out.println("JavaSparkContext ready.");
		// ...
		
		JavaRDD<Integer> numbersRDD = context.parallelize(
				Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15), 
				8); // 4 partitions
		
		System.out.println("----------");
		System.out.println("Initial RDD ( " + numbersRDD.getNumPartitions() + " partitions) : " );
		
		// "collect" :
		// this method should only be used if the resulting array is expected to be small, 
		// as all the data is loaded into the driver's memory.
		System.out.println(numbersRDD.collect().toString());
		
		
		System.out.println("----------");
		
		// 4 partitions => 4 tasks 
		numbersRDD.foreach(item -> {  // "foreach" is an ACTION 
		    TaskContext tc = TaskContext.get();
		    long taskId = tc.taskAttemptId();
		    int partitionId = TaskContext.getPartitionId();
            System.out.println("* " + item 
            		+ " (Partition id : " + partitionId + ")" 
            		+ " (Task id : " + taskId + ")"); 
        });
		
		System.out.println("----------");
		System.out.println("Closing...");
		context.close();
	}
}
