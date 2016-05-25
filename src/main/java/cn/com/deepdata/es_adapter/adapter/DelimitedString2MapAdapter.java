package cn.com.deepdata.es_adapter.adapter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adapter that adapts delimited string to 
 * JSON in Map format.
 */
public class DelimitedString2MapAdapter extends AbstractAdapter {
	
	protected List<String> titleList;
	
	protected String delimiterRegex;
	
	protected boolean shouldTrimValue;
	
	public DelimitedString2MapAdapter(List<String> titleList, String delimiterRegex) {
		this(titleList, delimiterRegex, true);
	}
	
	public DelimitedString2MapAdapter(List<String> titleList, String delimiterRegex, boolean shouldTrimValue) {
		this.titleList = Collections.unmodifiableList(titleList);
		this.delimiterRegex = delimiterRegex;
		this.shouldTrimValue = shouldTrimValue;
	}
	
	public Map<String, Object> adapter(String str) {
		String[] values = str.split(delimiterRegex);
		Map<String, Object> jsonMap = new HashMap<String, Object>();

		for (int i = 0; i < values.length; i++) {
			jsonMap.put(titleList.get(i), shouldTrimValue ? values[i].trim() : values[i]);
		}
		
		return jsonMap;
	}
	
	@Override
	public Object inboundAdapt(Object data, AdapterContext ctx) throws Exception {
		return adapter((String) data);
	}

	@Override
	public Object outboundAdapt(Object data, AdapterContext ctx) throws Exception {
		return adapter((String) data);
	}
	
}
