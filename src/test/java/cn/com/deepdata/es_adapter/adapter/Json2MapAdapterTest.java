package cn.com.deepdata.es_adapter.adapter;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unused")
public class Json2MapAdapterTest {
	
	private Json2MapAdapter adapter;
	
	@Before
	public void initialize() {
		adapter = new Json2MapAdapter();
	}
	
	@Test
	public void testJsonStr() throws Exception {
		String json = "{\"msg\":\"hello, world\"}";
		System.out.println(adapter.inboundAdapt(json, null));
	}
	
	@Test
	public void testJsonByte() throws Exception {
		byte[] json = "{\"msg\":\"hello, world\"}".getBytes();
		System.out.println(adapter.inboundAdapt(json, null));
	}
	
	private static class Json {
		private String msg;
		public String getMsg() {
			return msg;
		}
		public void setMsg(String msg) {
			this.msg = msg;
		}
	}
	
	@Test
	public void testJsonObj() throws Exception {
		Json json = new Json();
		json.setMsg("hello, world");
		System.out.println(adapter.inboundAdapt(json, null));
	}
	
	@Test
	public void testJsonArrayStr() throws Exception {
		String json = "[{\"msg\":\"hello, world\"},{\"msg\":\"hi, there\"}]";
		System.out.println(adapter.inboundAdapt(json, null));
	}

}
