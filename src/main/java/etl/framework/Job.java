package etl.framework;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class Job<T> {
	
	private final Class<T> beanClass ;
	private final String   jobName ;
	private final String   jobMaster ;
	private final String   filePath ;
	private final MapFunction<Row, T>  mappingFunction ;
	private final ForeachFunction<T> foreachFunction ;
	
	/**
	 * Constructor 
	 * @param beanClass
	 * @param jobName the 'application name' for the job (used as 'appName(jobName)' ) 
	 * @param jobMaster the 'master' configuration for the job e.g. 'local[4]' (used by 'master("local[4]")' ) 
	 * @param filePath
	 * @param mappingFunction
	 * @param foreachFunction
	 */
	public Job(Class<T> beanClass, String jobName, String jobMaster,  String filePath, 
			MapFunction<Row, T> mappingFunction, ForeachFunction<T> foreachFunction) {
		super();
		this.jobName   = jobName;
		this.jobMaster = jobMaster;
		this.filePath  = filePath ;
		this.beanClass = beanClass ;
		this.mappingFunction = mappingFunction ;
		this.foreachFunction = foreachFunction ;
	}

	protected void log(String msg) {
		System.err.flush();
		System.out.println("[LOG] " + msg);
		System.out.flush();
	}

	/**
	 * 
	 */
	protected void processFile() {
		
		log("--- Starting job");
		
		log("Creating SparkSession...");
    	SparkSession sparkSession = SparkSession.builder()
    			.appName(jobName)
    			.master(jobMaster) 
    			.getOrCreate();
		log("SparkSession ready.");
		
		log("Creating DataFrameReader...");
    	DataFrameReader dataFrameReader = sparkSession.read()
	    	.option("header",    "true")
	    	.option("delimiter", ";")
	    	.format("csv");
		log("DataFrameReader ready.");    	
		
		log("Loading file to initial Dataset (Dataset<Row>)...");
    	Dataset<Row> initialDataset = dataFrameReader.load(filePath);    	
		log("Dataset<Row> ready."); // Not realy loaded

		// TRANSFORMATION : map to PERSON 
		log("Mapping initial Dataset to DataSet of beans (Dataset<T>)...");
		Dataset<T> dataset = initialDataset.map(mappingFunction, Encoders.bean(beanClass) );
		log("Dataset<T> ready."); 
		
		// ACTION : foreach
    	log("Sarting 'foreach' action... " ); 	
    	dataset.foreach(foreachFunction);
    	log("End of 'foreach'. " ); 	

    	log("--- End of job");
	}
}
