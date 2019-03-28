package spark.dataset;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

public class SparkSample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSample")
                .master("local[*]")
                .getOrCreate();
        
        //Create Dataset
        List<Tuple2<String,Double>> inputList = new ArrayList<Tuple2<String,Double>>();
        inputList.add(new Tuple2<String,Double>("A",1.0));
        Dataset<Row> df = spark.createDataset(inputList, Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE())).toDF();
        df.show(false);
        
        //Java 8 style of creating Array. You can create by using for loop as well
        int[] array = IntStream.range(0, 5).toArray(); 
        //With Dummy Column
        Column newCol = explode(lit(array));
        Dataset<Row> df1 = df.withColumn("dummy", newCol);
        df1.show(false);
        
        
//        Column newCol = when(col("C").equalTo("A"), "X")
//        	    .when(col("C").equalTo("B"), "Y")
//        	    .otherwise("Z");
//        Dataset<Row> df1 = df.withColumn("dummy", newCol);
//        df1.show(false);
        
        //Drop Dummy Column
        Dataset<Row> df2 = df1.drop("dummy");
        df2.show(false);
    }
}
