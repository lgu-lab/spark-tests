package env;

import java.util.Properties;

public class Env {

	public static void println(String s) {
		System.out.println(s);
	}
	public static void main(String[] args) {

		println("JAVA VERSION : ");
		println("java.version : " + System.getProperty("java.version"));
		println("java.specification.version : " + System.getProperty("java.specification.version"));
		println("");
		println("ALL : ");
		Properties p = System.getProperties();
		for ( Object key : p.keySet() ) {
			println(" . " + key + " = " + p.get(key) );
		}
	}

}
