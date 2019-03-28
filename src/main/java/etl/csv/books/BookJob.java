package etl.csv.books;

import java.util.HashMap;
import java.util.Map;

import etl.framework.JobScriptContextRow;

/**
 * Job definition to process a file 
 * 
 * @author laguerin
 *
 */
public class BookJob extends JobScriptContextRow {
	
	/**
	 * Input file path
	 */
	private final static String INPUT_FILE_PATH = "D:/TMP/csv-files/books.csv";
	// Other example : "hdfs://localhost:9002/data/data.csv"
	
	/**
	 * Output file path
	 */
	private final static String OUTPUT_FILE_PATH = "D:/TMP/csv-files/books-result.csv";
	
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
	public BookJob() {
		
		super(	"PersonJob", 
				"local[4]", 
				INPUT_FILE_PATH
			);
	}

	public static void main(String[] args) throws Exception {
		
		String script = "" 
				+ "print('In Javascript');"
				+ "// Compute  \n"
				+ "id = id + 100 ; \n"
				;

		// Job initialization 
		BookJob job = new BookJob();
		job.setReaderOptions(readerOptions);

		//Dataset<Person> ds = job.getDataset();

		// Job actions ...
		
//		long count = job.count();
//		System.out.println("count = " + count);
//
//		Person person = job.first();
//		System.out.println("First is " + person);

		job.foreach( new BookForeachFunction(script) );
		
//		Dataset<String> dsJSON = job.toJSON();
//		System.out.println("First is " + dsJSON.first());
//	
//		job.save("hdfs://localhost:9002/data/data.csv");
		job.save(OUTPUT_FILE_PATH);

	}
}
