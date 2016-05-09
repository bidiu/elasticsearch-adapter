package cn.com.deepdata.es_adapter.adapter;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unused")
public class SimpleAttrNameAdapterTest {
	
	private SimpleAttrNameAdapter recursiveAdapter;
	private SimpleAttrNameAdapter adapter;
	
	@Before
	public void initialize() {
		Map<String, String> attrNameMap = new HashMap<String, String>();
		attrNameMap.put("objectOrigin", "objectNew");
	
		recursiveAdapter = new SimpleAttrNameAdapter(attrNameMap);
		adapter = new SimpleAttrNameAdapter(attrNameMap, false);
	}
	
	public Json getTestData() {
		return new Json("hello, world", "hi, there", new Json[] {
				new Json("aaaa", "bbbbb", null)	
		});
	}
	
	@Test
	public void testRecursive() throws Exception {
		Json json = getTestData();
		System.out.println(recursiveAdapter.inboundAdapt(json, null));
		System.out.println(adapter.inboundAdapt(json, null));
	}
	
	public static class Json {
		private String objectOrigin;
		private String attr1;
		private Json[] jsonArray;
		
		public Json(String objectOrigin, String attr1, Json[] jsonArray) {
			this.objectOrigin = objectOrigin;
			this.attr1 = attr1;
			this.jsonArray = jsonArray;
		}

		public String getObjectOrigin() {
			return objectOrigin;
		}
		public void setObjectOrigin(String objectOrigin) {
			this.objectOrigin = objectOrigin;
		}
		public String getAttr1() {
			return attr1;
		}
		public void setAttr1(String attr1) {
			this.attr1 = attr1;
		}
		public Json[] getJsonArray() {
			return jsonArray;
		}
		public void setJsonArray(Json[] jsonArray) {
			this.jsonArray = jsonArray;
		}
	}

}
