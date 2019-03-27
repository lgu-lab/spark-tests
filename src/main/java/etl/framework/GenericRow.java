package etl.framework;

import java.util.HashMap;
import java.util.Map;

public class GenericRow {

	private final Map<String,Object> map ;

	public GenericRow() {
		super();
		this.map = new HashMap<String,Object>();
	}
	
	public Map<String,Object> getMap() {
		return map ;
	}

	@Override
	public String toString() {
		return "GenericRow [map=" + map + "]";
	}
	
}
