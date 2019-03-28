package etl.csv.books;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;

import etl.framework.AbstractScriptExecutor;
import etl.framework.GenericRow;

public class BookForeachFunction extends AbstractScriptExecutor implements ForeachFunction<Row> {

	private static final long serialVersionUID = 1L;

	public BookForeachFunction() {
		super();
	}

	public BookForeachFunction(String script) throws Exception {
		super(script);
	}
	
	@Override
	public void call(Row row) throws Exception {
		
		// Get Spark partition ID 
		int partitionId = TaskContext.getPartitionId(); // get from ThreadLocal
		long taskId = TaskContext.get().taskAttemptId(); 
		System.out.println("* processing row (Task:" + taskId + "/Partition:" + partitionId + ") ... " );

		// DO SOMETHING HERE WITH THE GIVEN BEAN INSTANCE....
		System.out.println("  row before script : " + row);

		// TRANSFORM THE CURRENT ROW USING A SCRIPT 
		//executeScript(genericRow.getMap());
		double price = Double.parseDouble( row.<String>getAs(2).trim() );
		price = price + 100.00;
		// Cannot change the Row here 
		
		System.out.println("  row after script  : " + row);
	}

}
