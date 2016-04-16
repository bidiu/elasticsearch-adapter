package cn.com.deepdata.es_adapter;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
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
		int total = 100000;
		PipelineSettings settings = PipelineSettings.builder()
				.index("index-test111")
				.type("type-test111")
				.build();
		Pipeline pipeline = Pipeline.build(settings);
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < total; i++) {
			pipeline.putData(getDoc());
			System.out.println(String.format("%.2f%%", ((double) i) / total * 100));
		}
		pipeline.close();
		long end = System.currentTimeMillis();
		
		System.out.println("********************************");
		System.out.println((end - start) / 1000 + "s");
	}

}
