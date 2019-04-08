package etl.csv.person.generic2;

import java.util.HashMap;
import java.util.Map;

import etl.framework.JobScriptContextRow;

/**
 * Job definition to process a file 
 * 
 * @author laguerin
 *
 */
public class PersonJob extends JobScriptContextRow {
	
	/**
	 * Input file path
	 */
	private final static String INPUT_FILE_PATH = "D:/TMP/csv-files/person.csv";
	// Other example : "hdfs://localhost:9002/data/data.csv"
	
	/**
	 * Output file path
	 */
	private final static String OUTPUT_FILE_PATH = "D:/TMP/csv-files/person-result.csv";
	
	/**
	 * Input file reader options
	 */
	private final static Map<String,String> readerOptions = new HashMap<>();
	static {
		readerOptions.put("header",     "true");
		readerOptions.put("delimiter",  ";");
	}
	
	/**
	 * Job constructor
	 */
	public PersonJob() {
		
//		super(	"PersonJob", 
//				"local[4]", 
//				INPUT_FILE_PATH, 
//				new PersonMapFunction() );
		super(	"PersonJob", 
				"local[4]", 
				INPUT_FILE_PATH );
	}

	public static void main(String[] args) throws Exception {
		
		String script = "" 
				+ "print('In Javascript');"
				+ "// Compute  \n"
				+ "id = id + 100 ; \n"
				;

		// Job initialization 
		PersonJob job = new PersonJob();
		job.setReaderOptions(readerOptions);

		//Dataset<Person> ds = job.getDataset();

		// Job actions ...
		
//		long count = job.count();
//		System.out.println("count = " + count);
//
//		Person person = job.first();
//		System.out.println("First is " + person);

		job.foreach( new PersonForeachFunction(script) );
		
//		Dataset<String> dsJSON = job.toJSON();
//		System.out.println("First is " + dsJSON.first());
//	

	}
}
