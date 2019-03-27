package etl.framework;

import java.io.Serializable;
import java.util.Map;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * This class MUST BE SERIALIZABLE in order to be used with Spark 
 * 
 * NB : NashornScriptEngine is not serializable 
 *   Error :
 *     Exception in thread "main" org.apache.spark.SparkException: Task not serializable
 *     Caused by: java.io.NotSerializableException: jdk.nashorn.api.scripting.NashornScriptEngine$3
 * 
 * So the script must be stored in a String (and compiled after Serialization/Deserialization)
 * 
 * @author l.guerin
 *
 */
public abstract class AbstractScriptExecutor implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private final String         script ;
	private       CompiledScript compiledScript ;
	
	public AbstractScriptExecutor() {
		super();
		this.script = null ;
		this.compiledScript = null ;
	}
	
	public AbstractScriptExecutor(String script) throws Exception {
		super();
		this.script = script ;
		this.compiledScript = null ;
	}

	private CompiledScript getCompiledScript() throws Exception {
		if ( compiledScript != null ) {
			return compiledScript;
		}
		else {
			if ( script != null ) {
				ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
				ScriptEngine scriptEngine = scriptEngineManager.getEngineByName("javascript");
				Compilable compilable = (Compilable) scriptEngine;
				
				try {
					compiledScript =  compilable.compile(script);
				} catch (ScriptException e) {
					throw new Exception("Script compilation error.", e);
				}
			}
			return compiledScript;
		}
	}
	
	public void executeScript( Map<String,Object> map ) throws Exception {
		
		CompiledScript cs = getCompiledScript();
		if ( cs != null ) {
			ScriptEngine scriptEngine = compiledScript.getEngine();
			
			// Put variables in JS engine context ( map --> context )
			for ( Map.Entry<String,Object> entry : map.entrySet() ) {
				scriptEngine.put(entry.getKey(), entry.getValue());			
			}
			
			// Run script evaluation 
			try {
				cs.eval();
			} catch (ScriptException e) {
				throw new Exception("Script eval error.", e);
			} 

			// Get variables from JS engine context ( context --> map )
			for ( Map.Entry<String,Object> entry : map.entrySet() ) {
				entry.setValue(scriptEngine.get(entry.getKey()));
			}
		}
	}
	
}
