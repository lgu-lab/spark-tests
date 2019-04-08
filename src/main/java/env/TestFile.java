package env;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestFile {

	// Standard file
	private final static String FILE_PATH = "D:/TMP/csv-files/books.csv" ;

	// HDFS file : "hdfs://host:port/file"
	//private final static String FILE_PATH = "hdfs://user/hdfs/my_props.txt" ;
	//private final static String FILE_PATH = "hdfs://host:9000/mypath/myfile.txt" ;
			
	public static void main(String[] args) throws Exception {

		System.out.println("Trying to load file " + FILE_PATH );
		Path path = new Path(FILE_PATH);
		FileSystem fs = FileSystem.get(new Configuration());
		System.out.println("FileSystem ready.");

		InputStreamReader isr = new InputStreamReader(fs.open(path)) ;
//		InputStreamReader isr = getInputStreamReader(FILE_PATH);
		
		System.out.println("Starting reading...");
		readAllLines( isr ) ;
		System.out.println("Normal end.");
	}
	
	public static InputStreamReader getInputStreamReader(String filePath) throws Exception  {
		
		if ( filePath.startsWith("hdfs:") ) {
			System.out.println("HDFS file path");
			Path path = new Path(filePath);
			FileSystem fs = FileSystem.get(new Configuration());
			System.out.println("FileSystem ready.");
			return new InputStreamReader(fs.open(path)) ;
		}
		else {
			InputStream inputStream = new FileInputStream(filePath);
			return new InputStreamReader(inputStream);
		}
	}
	
	public static void readAllLines(InputStreamReader inputStreamReader) throws Exception  {
		BufferedReader br = new BufferedReader(inputStreamReader);
		try {
			String line;
			while ( ( line = br.readLine() ) != null) {
				System.out.println("> " +line);
			}
		} finally {
			br.close();
		}
	}
}
