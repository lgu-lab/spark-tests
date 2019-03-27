package etl.csv.context.person;

import java.util.HashMap;
import java.util.Map;

import etl.framework.JobScriptContext;

/**
 * Job definition to process a file 
 * 
 * @author laguerin
 *
 */
public class PersonJob extends JobScriptContext {
	
	/**
	 * Input file path
	 */
	private final static String FILE_PATH = "D:/TMP/csv-files/person.csv";
	
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
		
		super(	"PersonJob", 
				"local[4]", 
				FILE_PATH, 
				new PersonMapFunction() );
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
//		job.write();

	}
}
