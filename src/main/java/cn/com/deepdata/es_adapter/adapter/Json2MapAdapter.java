package cn.com.deepdata.es_adapter.adapter;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Adapter that adapts JSON in either String, byte array, or Java Object 
 * format to Java Map.
 * <p/>
 * If the given JSON is an array, then the resulting type is List<Object>, 
 * otherwise it's Map<String, Object>.
 * <p/>
 * If exception occurs while trying to adapt the data, the exception will be 
 * propagated along the adapter chain.
 */
public class Json2MapAdapter extends AbstractAdapter {
	
	protected static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();
	
	protected ObjectMapper objectMapper;
	
	public Json2MapAdapter() {
		this(DEFAULT_OBJECT_MAPPER);
	}
	
	/**
	 * Call this constructor if you want to customize the 
	 * {@link ObjectMapper} that the adapter uses.
	 * 
	 * @param objectMapper
	 */
	public Json2MapAdapter(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}
	
	/**
	 * Adapt Java Object to Java Map.
	 * 
	 * @param data
	 * @return
	 * @throws Exception
	 */
	protected Object adapt(Object data) throws Exception {
		if (data instanceof Map) {
			return data;
		}
		else if (data instanceof byte[]) {
			return objectMapper.readValue((byte[]) data, Object.class);
		}
		else if (data instanceof String) {
			return objectMapper.readValue((String) data, Object.class);
		}
		else {
			// here, JSON in Java object format
			String json = objectMapper.writeValueAsString(data);
			return objectMapper.readValue(json, Object.class);
		}
	}
	
	@Override
	public Object inboundAdapt(Object data, AdapterContext ctx) throws Exception {
		return adapt(data);
	}

	@Override
	public Object outboundAdapt(Object data, AdapterContext ctx) throws Exception {
		return adapt(data);
	}
	
}
