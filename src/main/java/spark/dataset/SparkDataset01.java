package spark.dataset;

import java.util.Hashtable;
import java.util.Map;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SparkDataset01 {
	
	private static Map<Integer,Boolean> done   = new Hashtable<>() ;
	private static Map<Long,Long>    taskCount = new Hashtable<>() ; // Nb of items processed by task

	public static void flushOutput() {
		System.err.flush();
		System.out.flush();
	}
	
	public static void main(String[] args) {

		System.out.println("Starting...");
		
    	SparkSession sparkSession = SparkSession.builder()
    			.appName("SparkTests")
//    			.master("local") // default : 1 partition --> 1 task
//    			.master("local[2]") // 2 partitions --> 2 tasks => 82 sec
    			.master("local[4]") // 4 partitions --> 4 tasks => 41 sec
//    			.master("local[6]") // 6 partitions --> 6 tasks => 27 sec 
//    			.master("local[12]") // real : 8 partitions --> 8 tasks (limitation ?) => 20 sec
    			.getOrCreate();
    	
		System.out.println("SparkSession ready.");
    	
    	String csv_file = "D:/TMP/csv-files/GLDSOLD.CSV";
    	
		
		System.out.println("Creating DataFrameReader...");    	
    	DataFrameReader dfr = sparkSession.read()
	    	.option("header",    "true")
	    	.option("delimiter", "|")
	    	.format("csv");
		System.out.println("DataFrameReader ready.");    	
		flushOutput();
		
		System.out.println("Loading file...");
    	Dataset<Row> dataset = dfr.load(csv_file);    	
		System.out.println("File loaded."); // Not realy loaded
		flushOutput();
		
		System.out.println("Get schema : dataset.schema()...");
    	StructType datasetSchema =  dataset.schema() ;
    	System.out.println("Dataset schema : " + datasetSchema );
		flushOutput();

		// ACTION
		System.out.println("Get count : dataset.count()...");
    	long datasetCount = dataset.count() ;  // ACTION => read file (N partitions => N read tasks )
    	System.out.println("Dataset count  : " + datasetCount);
		flushOutput();

/* LOG example (reading is triggered at the first "ACTION" ) : 
19/03/22 13:52:16 INFO TaskSchedulerImpl: Adding task set 1.0 with 4 tasks
19/03/22 13:52:16 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, executor driver, partition 0, PROCESS_LOCAL, 8290 bytes)
19/03/22 13:52:16 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 2, localhost, executor driver, partition 1, PROCESS_LOCAL, 8290 bytes)
19/03/22 13:52:16 INFO TaskSetManager: Starting task 2.0 in stage 1.0 (TID 3, localhost, executor driver, partition 2, PROCESS_LOCAL, 8290 bytes)
19/03/22 13:52:16 INFO TaskSetManager: Starting task 3.0 in stage 1.0 (TID 4, localhost, executor driver, partition 3, PROCESS_LOCAL, 8290 bytes)
19/03/22 13:52:16 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
19/03/22 13:52:16 INFO Executor: Running task 1.0 in stage 1.0 (TID 2)
19/03/22 13:52:16 INFO Executor: Running task 2.0 in stage 1.0 (TID 3)
19/03/22 13:52:16 INFO Executor: Running task 3.0 in stage 1.0 (TID 4)
19/03/22 13:52:16 INFO FileScanRDD: Reading File path: file:///D:/TMP/csv-files/GLDSOLD.CSV, range: 0-8695600, partition values: [empty row]
19/03/22 13:52:16 INFO FileScanRDD: Reading File path: file:///D:/TMP/csv-files/GLDSOLD.CSV, range: 17391200-26086800, partition values: [empty row]
19/03/22 13:52:16 INFO FileScanRDD: Reading File path: file:///D:/TMP/csv-files/GLDSOLD.CSV, range: 8695600-17391200, partition values: [empty row]
19/03/22 13:52:16 INFO FileScanRDD: Reading File path: file:///D:/TMP/csv-files/GLDSOLD.CSV, range: 26086800-30588096, partition values: [empty row]
*/
		
    	
    	System.out.println("Dataset isLocal ? : " + dataset.isLocal() );
    	System.out.println("Dataset isStreaming ? : " + dataset.isStreaming() );
		flushOutput();
    	
//    	System.out.println("Getting JavaRDD..." );
//    	JavaRDD<Row> javaRDD = dataset.javaRDD();
//    	System.out.println("JavaRDD ready." );
//		flushOutput();
//    	System.out.println("Dataset RDD partitions : " + javaRDD.getNumPartitions() );
//		flushOutput();
    	

    	System.out.println("Getting first..." );
    	Row row = dataset.first();
    	System.out.println("First Row : " + row );
		flushOutput();
    	System.out.println("Row length : " + row.length() );
    	System.out.println("Col 0 : " + row.getString(0) ); 	
    	//System.out.println("Col 1 : " + row.getInt(1) ); // ClassCastException: java.lang.String cannot be cast to java.lang.Integer
    	System.out.println("Col 1 : " + row.getString(1) ); 	
		flushOutput();

		// ACTION
    	System.out.println("foreach : starting tasks... " ); 	
    	long startTime = System.currentTimeMillis();
    	System.out.println("startTime : " + startTime ); 	
		flushOutput();
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
	    		flushOutput();
		    }
            Thread.sleep(10); // millisecond
            Long n ;
    		if ( ( n = taskCount.get(taskId) ) == null ) {
    			taskCount.put(taskId, 1L);
    		}
    		else {
    			taskCount.put(taskId, n+1);
    		}
        });
    	System.out.println("foreach : done. " ); 	
		flushOutput();
    	
    	long endTime  = System.currentTimeMillis();
    	System.out.println("endTime : " + endTime ); 	
    	long duration = (endTime - startTime) / 1000; // in seconds
    	System.out.println("duration : " + duration + " seconds"); 	
		flushOutput();
    	
    	/*
    	 * With "local[4]" => 4 partitions
    	 * Partition 0 --> Task  7
    	 * Partition 1 --> Task  8
    	 * Partition 2 --> Task  9
    	 * Partition 3 --> Task 10
    	 */
    	
    	long sum = 0 ;
    	for (Map.Entry<Long, Long> entry : taskCount.entrySet()) {
    	    System.out.println("Task " + entry.getKey() + " : " + entry.getValue() + " items processed");
    	    sum += entry.getValue() ;
    	}
	    System.out.println("Sum (all items) :  " + sum );
		flushOutput();
	}
	
}
