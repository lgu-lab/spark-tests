package spark.dataset;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.ForeachFunction;

public class PersonProcessing implements ForeachFunction<Person> {

	private static final long serialVersionUID = 1L;

	@Override
	public void call(Person person) throws Exception {
		
		// DO SOMETHING HERE WITH THE GIVEN INSTANCE....
		
		// Get Spark partition ID 
		int partitionId = TaskContext.getPartitionId(); // get from ThreadLocal
		long taskId = TaskContext.get().taskAttemptId(); 
		
		System.out.println("* processing person (Task:" + taskId + "/Partition:" + partitionId + ") : " + person );
	}

}
