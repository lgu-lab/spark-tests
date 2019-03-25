package spark.dataset;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class PersonMapping implements MapFunction<Row, Person> {

	private static final long serialVersionUID = 1L;

	@Override
	public Person call(Row row) throws Exception {
		int id = Integer.parseInt( row.<String>getAs(0).trim() );
		String firstName = row.<String>getAs(1).trim() ;
		String lastName = row.<String>getAs(2).trim() ;
		return new Person(id, firstName, lastName) ;
	}

}
