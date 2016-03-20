package cn.com.deepdata.es_adapter;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import jdk.nashorn.internal.ir.annotations.Ignore;

import org.junit.Test;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;

@SuppressWarnings("unused")
public class PipelineTest {
	
	public Map<String, Object> getDoc() {
		Map<String, Object> doc = new HashMap<String, Object>();
		doc.put("isbn", "1001");
		doc.put("title", "Thinking in Java");
		return doc;
	}
	
	@Ignore
	@Test
	public void test() throws InterruptedException {
		PipelineSettings settings = PipelineSettings.getDefaultSettings()
				.index("library")
				.type("book");
		Pipeline pipeline = Pipeline.build(settings);
		pipeline.putData(getDoc());
		pipeline.close();
	}

}
