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
	
	protected final List<String> titleList;
	
	protected final String delimiterRegex;
	
	protected final boolean shouldTrimValue;
	
	protected final boolean shouldConvertEmptyStrToNull; 
	
	public DelimitedString2MapAdapter(List<String> titleList, String delimiterRegex) {
		this(titleList, delimiterRegex, true, true);
	}
	
	public DelimitedString2MapAdapter(List<String> titleList, String delimiterRegex, boolean shouldTrimValue, boolean shouldConvertEmptyStrToNull) {
		this.titleList = Collections.unmodifiableList(titleList);
		this.delimiterRegex = delimiterRegex;
		this.shouldTrimValue = shouldTrimValue;
		this.shouldConvertEmptyStrToNull = shouldConvertEmptyStrToNull;
	}
	
	public Map<String, Object> adapter(String str) {
		String[] values = str.split(delimiterRegex, -1);
		Map<String, Object> jsonMap = new HashMap<String, Object>();

		for (int i = 0; i < values.length; i++) {
			String value = shouldTrimValue ? values[i].trim() : values[i];
			if (shouldConvertEmptyStrToNull && value.isEmpty()) {
				jsonMap.put(titleList.get(i), null);
			}
			else {
				jsonMap.put(titleList.get(i), value);
			}
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
