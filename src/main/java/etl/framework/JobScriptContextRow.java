package etl.framework;

import java.util.Map;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public abstract class JobScriptContextRow {
	
	private final String   jobName ;
	private final String   jobMaster ;
	private final String   filePath ;
//	private final MapFunction<Row, Row>  mappingFunction ;
	private Map<String,String> dataFrameReaderOptions = null ;
	
	private Dataset<Row> dataset = null ;
	
	/**
	 * Constructor 
	 * @param jobName the 'application name' for the job (used as 'appName(jobName)' ) 
	 * @param jobMaster the 'master' configuration for the job e.g. 'local[4]' (used by 'master("local[4]")' ) 
	 * @param filePath
	 * @param mappingFunction
	 */
	public JobScriptContextRow(String jobName, String jobMaster,  String filePath ) {
//			MapFunction<Row, Row> mappingFunction) {
		super();
		this.jobName   = jobName;
		this.jobMaster = jobMaster;
		this.filePath  = filePath ;
//		this.mappingFunction = mappingFunction ;
	}

	protected void log(String msg) {
		System.err.flush();
		System.out.println("[LOG] " + msg);
		System.out.flush();
	}

	public void setReaderOptions(Map<String,String> options) {
		this.dataFrameReaderOptions = options ;
	}
	
	private Dataset<Row> createDataset() {
		
		log("Creating SparkSession...");
    	SparkSession sparkSession = SparkSession.builder()
    			.appName(jobName)
    			.master(jobMaster) 
    			.config("spark.ui.enabled", false)
    			.getOrCreate();
		log("SparkSession ready.");
		
		log("Creating DataFrameReader...");
    	DataFrameReader dataFrameReader = sparkSession.read()
	    	.option("header",    "true")
	    	.option("delimiter", ";")
	    	.format("csv");
    	
		log("Applying DataFrameReader options...");
    	dataFrameReader.options(this.dataFrameReaderOptions);
    	
		log("DataFrameReader ready.");    	
		
		log("Loading file to initial Dataset (Dataset<Row>)...");
    	Dataset<Row> initialDataset = dataFrameReader.load(filePath);    	
		log("Dataset<Row> ready."); // Not realy loaded

//		// TRANSFORMATION : map to JAVA BEAN 
//		log("Mapping initial Dataset to DataSet of beans (Dataset<Row>)...");
//		Dataset<Row> dataset = initialDataset.map(mappingFunction, Encoders.bean(Row.class) );
//		log("Dataset<Row> ready."); 
		
		dataset = initialDataset ;
		
		return dataset ;
	}
	
	protected Dataset<Row> getDataset() {
		log("getDataset()" ); 	
		if ( dataset == null ) {
			dataset = createDataset();
		}
		return dataset ;
	}
	
	/**
	 * SPARK ACTION : 'foreach'
	 */
	protected void foreach(ForeachFunction<Row> foreachFunction) {
		
		log("Sarting 'foreach' action... " ); 	
		Dataset<Row> dataset = getDataset();
    	dataset.foreach(foreachFunction);
    	log("End of 'foreach' action. " ); 	
	}

	/**
	 * SPARK ACTION : 'count'
	 * @return
	 */
	protected long count() {
		
		log("Sarting 'count' action... " ); 	
		Dataset<Row> dataset = getDataset();
    	long count = dataset.count();
    	log("End of 'count' action (count=" + count + "). " );
    	return count ;
	}

	/**
	 * SPARK ACTION : 'toJSON'
	 * @return
	 */
	protected Dataset<String> toJSON() {
		
		log("Sarting 'toJSON' action... " ); 	
		Dataset<Row> dataset = getDataset();
		Dataset<String> result = dataset.toJSON();
    	log("End of 'toJSON' action." );
    	return result ;
	}

	/**
	 * SPARK ACTION : 'first'
	 * @return
	 */
	protected Row first() {
		
		log("Sarting 'first' action... " ); 	
		Dataset<Row> dataset = getDataset();
		Row item = dataset.first();
    	log("End of 'first' action. " );
    	return item ;
	}

	protected void write() {
		
		log("Sarting 'write' action... " ); 	
		Dataset<Row> dataset = getDataset();
    	dataset.write();
    	log("End of 'write' action. " );
	}

	protected void save(String filePath) {
		
		log("Sarting 'write' action... " ); 	
		Dataset<Row> dataset = getDataset();
    	//dataset.write();
    	
		if ( ! dataset.isEmpty() ) {
			log("Dataset is not empty. Trying to save..." ); 	
	    	dataset.write()
	    	.format("com.databricks.spark.csv")
//	    	.option("inferSchema", "false")
	    	.option("header", "true")
	    	.option("charset", "UTF-8")
//	    	.option("escape", escape)
	    	.option("delimiter", "|")
	    	.option("quote", "\'")
	    	.mode(SaveMode.Overwrite)
	    	.save(filePath);
		}
		else {
			log("dataset is empty!" ); 	
		}
    	
    	log("End of 'write' action. " );
	}
	
}
