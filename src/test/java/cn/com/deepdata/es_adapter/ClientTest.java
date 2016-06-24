package cn.com.deepdata.es_adapter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.AbstractAdapter;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.adapter.AdapterContext;
import cn.com.deepdata.es_adapter.adapter.SimpleXlsFile2MapsAdapter;

public class ClientTest {

	@Ignore
	@Test
	public void test() throws InterruptedException {
		PipelineSettings settings = PipelineSettings.builder()
				.index("xls-index")
				.type("xls-type")
				.timeoutAfterClosing(5)
				.threadPoolSize(1)
				.build();
		Pipeline pipeline = Pipeline.build(settings, new AdapterChainInitializer() {
			
			@Override
			public void initialize(AdapterChain adapterChain) {
				List<String> titleList = new ArrayList<String>();
				titleList.add("a");
				titleList.add("b");
				titleList.add("c");
				titleList.add("d");
				adapterChain.addLast(new SimpleXlsFile2MapsAdapter(titleList, 1));
				adapterChain.addLast(new AbstractAdapter() {
					
					private final ObjectMapper objectMapper = new ObjectMapper();
					
					@Override
					public Object outboundAdapt(Object data, AdapterContext ctx) throws Exception {
						return data;
					}
					
					@Override
					public Object inboundAdapt(Object data, AdapterContext ctx) throws Exception {
						System.out.println(objectMapper.writeValueAsString(data));
						return data;
					}
				
				});
			}
			
		});
		pipeline.putData(new File("C:\\Users\\deepdata\\Desktop\\test.xls"));
		pipeline.close();
	}

}
