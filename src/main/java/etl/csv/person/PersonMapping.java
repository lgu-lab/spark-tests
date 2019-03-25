package etl.csv.person;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

class PersonMapping implements MapFunction<Row, Person> {

	private static final long serialVersionUID = 1L;

	@Override
	public Person call(Row row) throws Exception {
		
		// Get each bean attribute from CSV column
		int id = Integer.parseInt( row.<String>getAs(0).trim() );
		String firstName = row.<String>getAs(1).trim() ;
		String lastName = row.<String>getAs(2).trim() ;

		// Returns the Java bean
		return new Person(id, firstName, lastName) ;
	}

}
