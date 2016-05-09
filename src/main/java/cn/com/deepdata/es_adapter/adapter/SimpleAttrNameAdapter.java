package cn.com.deepdata.es_adapter.adapter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A simple adapter that adapts JSON's attribute names.
 * <p/>
 * The type of input data could be String, byte array, or Java object, while 
 * the type of output data is Map<String, Object>, or List<Object> if the given 
 * JSON is with value of array.
 * 
 * @author sunhe
 * @date 2016年5月6日
 */
public class SimpleAttrNameAdapter extends Json2MapAdapter {
	
	protected Map<String, String> attrNameMap;
	
	// cannot be altered once instantiated
	protected boolean isRecursive;
	
	public SimpleAttrNameAdapter(Map<String, String> attrNameMap) {
		this(attrNameMap, true, DEFAULT_OBJECT_MAPPER);
	}
	
	public SimpleAttrNameAdapter(Map<String, String> attrNameMap, boolean isRecursive) {
		this(attrNameMap, isRecursive, DEFAULT_OBJECT_MAPPER);
	}
	
	public SimpleAttrNameAdapter(Map<String, String> attrNameMap, ObjectMapper objectMapper) {
		this(attrNameMap, true, objectMapper);
	}
	
	public SimpleAttrNameAdapter(Map<String, String> attrNameMap, boolean isRecursive, ObjectMapper objectMapper) {
		super(objectMapper);
		this.attrNameMap = Collections.unmodifiableMap(attrNameMap);
		this.isRecursive = isRecursive;
	}
	
	protected void adaptList(List<Object> list) {
		for (Object ele : list) {
			if (ele instanceof Map) {
				@SuppressWarnings("unchecked")
				Map<String, Object> map = (Map<String, Object>) ele;
				adaptMap(map, false);
			}
			else if (ele instanceof List && isRecursive) {
				@SuppressWarnings("unchecked")
				List<Object> list2 = (List<Object>) ele;
				adaptList(list2);
			}
		}
	}
	
	protected void adaptMap(Map<String, Object> map, boolean isFirstLevel) {
		if (!isFirstLevel && !isRecursive) {
			return;
		}
		
		Map<String, String> adaptedAttrNameMap = new HashMap<String, String>(); 
		for (Entry<String, Object> entry : map.entrySet()) {
			String attrName = entry.getKey();
			Object attrValue = entry.getValue();
			
			String newAttrName = attrNameMap.get(attrName);
			if (newAttrName != null) {
				adaptedAttrNameMap.put(attrName, newAttrName);
			}
			
			if (attrValue instanceof Map) {
				@SuppressWarnings("unchecked")
				Map<String, Object> map2 = (Map<String, Object>) attrValue;
				adaptMap(map2, false);
			}
			else if (attrValue instanceof List && isRecursive) {
				@SuppressWarnings("unchecked")
				List<Object> list = (List<Object>) attrValue;
				adaptList(list);
			}
		}
		
		// adapt attribute name
		for (Entry<String, String> entry : adaptedAttrNameMap.entrySet()) {
			String attrName = entry.getKey();
			String newAttrName = entry.getValue();
			
			Object attrValue = map.get(attrName);
			map.remove(attrName);
			map.put(newAttrName, attrValue);
		}
	}
	
	@Override
	protected Object adapt(Object data) throws Exception {
		data = super.adapt(data);
		if (data instanceof List && isRecursive) {
			@SuppressWarnings("unchecked")
			List<Object> list = (List<Object>) data;
			adaptList(list);
		}
		else if (data instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> map = (Map<String, Object>) data;
			adaptMap(map, true);
		}
		return data;
	}
	
	@Override
	public Object inboundAdapt(Object data, Map<String, Object> msg) throws Exception {
		return adapt(data);
	}
	
	@Override
	public Object outboundAdapt(Object data, Map<String, Object> msg) throws Exception {
		return adapt(data);
	}
	
}
