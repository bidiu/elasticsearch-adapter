package cn.com.deepdata.es_adapter.adapter;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import cn.com.deepdata.es_adapter.Pipeline;
import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;

@SuppressWarnings("unused")
public class DataListSplitterTest {
	
	public Map<String, Object> getDoc(String isbn) {
		Map<String, Object> doc = new HashMap<String, Object>();
		doc.put("isbn", isbn);
		doc.put("title", "Thinking in Java");
		return doc;
	}
	
	@Ignore
	@Test
	public void test() throws InterruptedException {
		PipelineSettings settings = PipelineSettings.builder()
				.index("splitter-test")
				.type("type")
				.build();
		Pipeline pipeline = Pipeline.build(settings, new AdapterChainInitializer() {
			
			@Override
			public void initialize(AdapterChain adapterChain) {
				adapterChain.addLast(new DataListSplitter<Object>());
				
				Map<String, String> attrNameMap = new HashMap<String, String>();
				attrNameMap.put("title", "book name");
				adapterChain.addLast(new SimpleAttrNameAdapter(attrNameMap));
			}
			
		});
		List<Object> dataList = new ArrayList<Object>();
		dataList.add(getDoc("0001"));
		dataList.add(getDoc("0002"));
		pipeline.putData(dataList);
		Thread.sleep(20 * 1000);
		pipeline.close();
	}

}
