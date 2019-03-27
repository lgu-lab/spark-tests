package etl.csv.person.bean;

import java.util.HashMap;
import java.util.Map;

import etl.framework.Job;

public class PersonJob extends Job<Person> {
	
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
		
		super(Person.class, 
				"PersonJob", 
				"local[4]", 
				FILE_PATH, 
				new PersonMapFunction() );
	}

	public static void main(String[] args) {
		
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

		job.foreach( new PersonForeachFunction() );
		
//		Dataset<String> dsJSON = job.toJSON();
//		System.out.println("First is " + dsJSON.first());
//	
//		job.write();

	}
}
