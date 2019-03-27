package evaluator.janino;

import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.ScriptEvaluator;

public class Eval1 {

	
	public static void main(String[] args) throws Exception {
		
		eval1();
		eval2();
		
	}

	private static void eval1() throws Exception {
		ExpressionEvaluator ee = new ExpressionEvaluator();
		ee.cook("3 + 4");
		Object r = ee.evaluate(null); 
		System.out.println("Result : " + r );
	}

	private static void eval2() throws Exception {
		ScriptEvaluator se = new ScriptEvaluator();
		se.cook(""
				+ "static void method1() {\n" 
				+ "  System.out.println(1);"
				+ "}\n"
				+ "method1();\n"
				+ ""
				+ ""
				+ ""
				);
		Object r = se.evaluate(null); 
		System.out.println("Result : " + r );
	}
}
