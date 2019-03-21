package spark.dataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDataset02 {

	public static void main(String[] args) {

		System.out.println("Starting...");
		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");

		JavaSparkContext context = new JavaSparkContext(sparkConf);

		System.out.println("JavaSparkContext ready.");
		// ...
		
		System.out.println("----------");
		System.out.println("Closing...");
		context.close();
	}
	
//	private static Dataset<Row> getPayloadFromCsv( final SparkSession sparkSession ) {
//
//        String query = "(select * from dbo.xxxxx";
//        Dataset<Row> payload = sparkSession
//                .read()
//                .format( "jdbc" )
//                .option( "url", "" )
//                .option( "dbtable", query )
//                .option( "password", "" )
//                .option( "user", "" )
//                .load();
//
//        return payload;
//    }
 	
}
