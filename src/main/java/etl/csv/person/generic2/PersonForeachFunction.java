package etl.csv.person.generic2;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;

import etl.framework.AbstractScriptExecutor;

public class PersonForeachFunction extends AbstractScriptExecutor implements ForeachFunction<Row> {

	private static final long serialVersionUID = 1L;

	public PersonForeachFunction() {
		super();
	}

	public PersonForeachFunction(String script) throws Exception {
		super(script);
	}
	
	@Override
	public void call(Row row) throws Exception {
		
		// Get Spark partition ID 
		int partitionId = TaskContext.getPartitionId(); // get from ThreadLocal
		long taskId = TaskContext.get().taskAttemptId(); 
		System.out.println("* processing person (Task:" + taskId + "/Partition:" + partitionId + ") ... " );

		
		// The 'row' always remains unchanged
		
		
		Map<String,Object> map = new HashMap<>();
		preProcessing(row, map);
		
		// DO SOMETHING HERE WITH THE GIVEN BEAN INSTANCE....
//		System.out.println("  row before script : " + row);
		System.out.println("  map before script : " + map);

		// TRANSFORM THE CURRENT ROW USING A SCRIPT 
		//executeScript(genericRow.getMap());
		executeScript(map);
		
		System.out.println("  map after script : " + map);
//		System.out.println("  row after script  : " + row);
		postProcessing(row, map);
		
		
	}

	public void preProcessing(Row row, Map<String,Object> map) throws Exception {
		System.out.println("  --- preProcessing ");
		map.put("id", Integer.parseInt( row.<String>getAs(0).trim() ) );
		map.put("firstName", row.<String>getAs(1).trim() );
		map.put("lastName", row.<String>getAs(2).trim() );
	}

	public void postProcessing(Row row, Map<String,Object> map) throws Exception {
		
		System.out.println("  --- postProcessing ");
	}
}
