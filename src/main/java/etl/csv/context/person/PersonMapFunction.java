package etl.csv.context.person;

import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import etl.framework.GenericRow;

class PersonMapFunction implements MapFunction<Row, GenericRow> {

	private static final long serialVersionUID = 1L;

	@Override
	public GenericRow call(Row row) throws Exception {
		
		GenericRow genericRow = new GenericRow();
		Map<String,Object> map = genericRow.getMap();
		
		// Get each bean attribute from CSV column
		map.put("id", Integer.parseInt( row.<String>getAs(0).trim() ) );
		map.put("firstName", row.<String>getAs(1).trim() );
		map.put("lastName", row.<String>getAs(2).trim() );
		
//		// Get Spark partition ID 
//		int partitionId = TaskContext.getPartitionId(); // get from ThreadLocal
//		long taskId = TaskContext.get().taskAttemptId(); 
		
//		System.out.println("* MapFunction (Task:" + taskId + "/Partition:" + partitionId + ") : " + person );
		return genericRow ;
	}

}
