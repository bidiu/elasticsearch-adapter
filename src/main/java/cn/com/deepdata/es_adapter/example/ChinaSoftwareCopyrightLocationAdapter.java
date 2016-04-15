package cn.com.deepdata.es_adapter.example;

import java.util.Map;

import cn.com.deepdata.es_adapter.adapter.AbstractAdapter;

/**
 * 
 * @author sunhe
 * @date 2016年4月5日
 */
public class ChinaSoftwareCopyrightLocationAdapter extends AbstractAdapter {
	
	@Override
	public Object inboundAdapt(Object data, Map<String, Object> msg) {
		@SuppressWarnings("unchecked")
		Map<String, Object> doc = (Map<String, Object>) data;
		doc.put("sn_country", "中国");
		doc.put("sn_province", msg.get("province"));
		doc.put("sn_city", msg.get("city"));
		return doc;
	}
	
	@Override
	public Object outboundAdapt(Object data, Map<String, Object> msg) {
		throw new IllegalStateException("Not implemented");
	}

}
