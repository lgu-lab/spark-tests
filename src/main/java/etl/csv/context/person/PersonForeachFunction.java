package etl.csv.context.person;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.ForeachFunction;

import etl.framework.AbstractScriptExecutor;
import etl.framework.GenericRow;

public class PersonForeachFunction extends AbstractScriptExecutor implements ForeachFunction<GenericRow> {

	private static final long serialVersionUID = 1L;

	public PersonForeachFunction() {
		super();
	}

	public PersonForeachFunction(String script) throws Exception {
		super(script);
	}
	
	@Override
	public void call(GenericRow genericRow) throws Exception {
		
		// Get Spark partition ID 
		int partitionId = TaskContext.getPartitionId(); // get from ThreadLocal
		long taskId = TaskContext.get().taskAttemptId(); 
		System.out.println("* processing person (Task:" + taskId + "/Partition:" + partitionId + ") ... " );

		// DO SOMETHING HERE WITH THE GIVEN BEAN INSTANCE....
		System.out.println("  row before script : " + genericRow);

		executeScript(genericRow.getMap());
		
		System.out.println("  row after script  : " + genericRow);
	}

}
