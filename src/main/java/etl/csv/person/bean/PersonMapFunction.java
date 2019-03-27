package etl.csv.person.bean;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

class PersonMapFunction implements MapFunction<Row, Person> {

	private static final long serialVersionUID = 1L;

	@Override
	public Person call(Row row) throws Exception {
		

		// Get each bean attribute from CSV column
		int id = Integer.parseInt( row.<String>getAs(0).trim() );
		String firstName = row.<String>getAs(1).trim() ;
		String lastName = row.<String>getAs(2).trim() ;

		// Creating the Java bean
		Person person = new Person(id, firstName, lastName) ;

		// Get Spark partition ID 
		int partitionId = TaskContext.getPartitionId(); // get from ThreadLocal
		long taskId = TaskContext.get().taskAttemptId(); 
		
		System.out.println("* MapFunction (Task:" + taskId + "/Partition:" + partitionId + ") : " + person );
		return person ;
	}

}
