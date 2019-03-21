package spark.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRDD03Actions {

	public static void main(String[] args) {

		System.out.println("Starting...");
		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");

		JavaSparkContext context = new JavaSparkContext(sparkConf);

		System.out.println("JavaSparkContext ready.");
		// ...

		JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
		System.out.println("----------");
		System.out.println("Initial RDD : ");
//		System.out.println(numbersRDD.collect().toString());

		System.out.println("----------");
		System.out.println("take(3) ... ");
		List<Integer> numbersList = numbersRDD.take(3); // from first to 3rd (ACTION)
		System.out.println(numbersList.toString());

		System.out.println("----------");
		System.out.println("take(5) ... ");
		List<Integer> list2 = numbersRDD.take(5); // from first to 5th (ACTION)
		System.out.println(list2.toString());

		System.out.println("----------");
		System.out.println("count() ... ");
		System.out.println(numbersRDD.count());

		//JavaRDD<Integer> sortedRDD = numbersRDD.sortBy(f, ascending, numPartitions);
//		JavaRDD<Integer> sortedRDD = numbersRDD.sortBy(new Function<Integer>() {
//			@Override
//			public Integer call(Integer arg0) throws Exception {
//				// TODO Auto-generated method stub
//				return arg0 ;
//			}
//		}, false, 10);
		
		System.out.println("----------");
		
		System.out.println("sortBy() ... ");
		// "SortBy" is a "transformation" but trigger a Spark job
		JavaRDD<Integer> sortedRDD = numbersRDD.sortBy(f -> {
            System.out.println("sort using item : " + f 
            		+ " (Partition id : " + TaskContext.getPartitionId() + ")" 
            		+ " (Task id : " + TaskContext.get().taskAttemptId() + ")");
			return f;
		}, false, 4); // descending, 4 partitions
		// if only 1 partition => only 1 task
		
		System.out.println("foreach() : print ");
		sortedRDD.foreach(item -> {  // "foreach" is an ACTION 
		    TaskContext tc = TaskContext.get();
		    long taskId = tc.taskAttemptId();
            System.out.println("* " + item 
            		+ " (Partition id : " + TaskContext.getPartitionId() + ")" 
            		+ " (Task id : " + taskId + ")"); 
        });

		System.out.println("----------");
		System.out.println("Closing...");
		context.close();
	}
}
