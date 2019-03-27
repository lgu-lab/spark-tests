package etl.csv.map;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import etl.csv.person.Person;

class ScriptContextMapFunction implements MapFunction<Row, Map<String,Object>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Map<String,Object> call(Row row) throws Exception {
		
		Map<String,Object> map = new HashMap<>();
		
		// Get each bean attribute from CSV column
		map.put("id", Integer.parseInt( row.<String>getAs(0).trim() ) );
		map.put("firstName", row.<String>getAs(1).trim() );
		map.put("lastName", row.<String>getAs(2).trim() );
		
//		// Get Spark partition ID 
//		int partitionId = TaskContext.getPartitionId(); // get from ThreadLocal
//		long taskId = TaskContext.get().taskAttemptId(); 
		
//		System.out.println("* MapFunction (Task:" + taskId + "/Partition:" + partitionId + ") : " + person );
		return map ;
	}

}
