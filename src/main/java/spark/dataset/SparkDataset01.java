package spark.dataset;

import java.util.Hashtable;
import java.util.Map;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDataset01 {
	
	// private static boolean part0 = false ;
	private static Map<Integer,Boolean> done = new Hashtable<>() ;
	private static Map<Integer,Long> count   = new Hashtable<>() ;

	public static void main(String[] args) {

		System.out.println("Starting...");
		
    	SparkSession sparkSession = SparkSession.builder()
    			.appName("SparkTests")
    			//.master("local") // default : 1 partition --> 1 task
//    			.master("local[6]") // 6 partitions --> 6 tasks
//    			.master("local[2]") // 2 partitions --> 2 tasks
    			.master("local[12]") // real : 8 partitions --> 8 tasks (limitation ?)
    			.getOrCreate();
    	
		System.out.println("SparkSession ready.");
    	
    	//String csv_file = "C:\\Users\\ybenabde\\Downloads\\csv\\large_file.csv";
    	String csv_file = "D:/TMP/csv-files/GLDSOLD.CSV";
    	
		
		System.out.println("Creating DataFrameReader...");    	
    	DataFrameReader dfr = sparkSession.read()
	    	.option("header",    "true")
	    	.option("delimiter", "|")
	    	.format("csv");
    	
		System.out.println("Loading file...");
    	Dataset<Row> dataset = dfr.load(csv_file);    	
		System.out.println("File loaded.");
		
    	System.out.println("Dataset count  : " + dataset.count() );
    	System.out.println("Dataset schema : " + dataset.schema() );
    	
    	System.out.println("Dataset isLocal ? : " + dataset.isLocal() );
    	System.out.println("Dataset isStreaming ? : " + dataset.isStreaming() );
    	
//    	System.out.println("Getting RDD..." );
//    	RDD<Row> rdd = dataset.rdd();
//    	System.out.println("RDD ready." );
//    	System.out.println("Dataset RDD partitions : " + rdd.getNumPartitions() );
    	
    	System.out.println("Getting JavaRDD..." );
    	JavaRDD<Row> javaRDD = dataset.javaRDD();
    	System.out.println("JavaRDD ready." );
    	System.out.println("Dataset RDD partitions : " + javaRDD.getNumPartitions() );
    	

    	Row row = dataset.first();
    	System.out.println("First Row : " + row );
    	System.out.println("Row length : " + row.length() );
    	System.out.println("Col 0 : " + row.getString(0) ); 	
    	//System.out.println("Col 1 : " + row.getInt(1) ); // ClassCastException: java.lang.String cannot be cast to java.lang.Integer
    	System.out.println("Col 1 : " + row.getString(1) ); 	

    	
    	System.out.println("foreach : starting tasks... " ); 	
    	long startTime = System.currentTimeMillis();
    	System.out.println("startTime : " + startTime ); 	
    	dataset.foreach(item -> {  // "foreach" is an ACTION 
    		
		    TaskContext tc = TaskContext.get();
		    long taskId = tc.taskAttemptId();
		    int partitionId = TaskContext.getPartitionId(); // get from ThreadLocal ?
		    if ( done.get(partitionId) == null ) {
	            System.out.println("* " 
        		+ " (Partition id : " + partitionId + ")" 
        		+ " (Stage id : " + tc.stageId() + ")"
        		+ " (Task id : " + taskId + ")"
        		);
	            done.put(partitionId, true);
//	            ClassLoader cl = TaskContext.class.getClassLoader();
//	            System.out.println("ClassLoader : " + cl);
	            System.out.flush();
		    }
            Thread.sleep(10); // milisecond
            Long n ;
    		if ( ( n = count.get(partitionId) ) == null ) {
    			count.put(partitionId, 1L);
    		}
    		else {
    			count.put(partitionId, n+1);
    		}
        });
    	System.out.println("foreach : done. " ); 	
    	long endTime  = System.currentTimeMillis();
    	System.out.println("endTime : " + endTime ); 	
    	long duration = (endTime - startTime) / 1000; // in seconds
    	System.out.println("duration : " + duration + " seconds"); 	
    	
    	/*
    	 * With "local[4]" => 4 partitions
    	 * Partition 0 --> Task  7
    	 * Partition 1 --> Task  8
    	 * Partition 2 --> Task  9
    	 * Partition 3 --> Task 10
    	 */
	}
	
}
