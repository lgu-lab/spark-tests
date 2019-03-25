package etl.csv.person;

import etl.framework.Job;

public class PersonJob extends Job<Person> {
	
	private final static String FILE_PATH = "D:/TMP/csv-files/person.csv";
	
	public PersonJob() {
		
		super(Person.class, 
				"PersonJob", 
				"local[4]", 
				FILE_PATH, 
				new PersonMapping(),
				new PersonProcessing() );
	}

	public static void main(String[] args) {
		PersonJob job = new PersonJob();
		job.processFile();
	}
}
