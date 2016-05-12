package cn.com.deepdata.es_adapter.example;

import java.util.HashMap;

import cn.com.deepdata.es_adapter.Pipeline;
import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.adapter.DataListSplitter;
import cn.com.deepdata.es_adapter.adapter.File2LinesAdapter;
import cn.com.deepdata.es_adapter.adapter.SimpleAttrNameAdapter;

/**
 * 
 * @author sunhe
 * @date 2016年5月12日
 */
public class WechatOfficalAccountTrans {
	
	public static void main(String[] args) throws InterruptedException {
		String file = "/home/hadoop/sunhe/sample.json";
		
		PipelineSettings settings = PipelineSettings.builder()
				.host("localhost")
				.port(19300)
				.index("flume-weixin-data")
				.type("weixin-data")
				.timeoutAfterClosing(15)
				.build();
		Pipeline pipeline = Pipeline.build(settings, new AdapterChainInitializer() {
			
			@Override
			public void initialize(AdapterChain adapterChain) {
				adapterChain.addLast(new File2LinesAdapter());
				
				HashMap<String, String> attrNameMap = new HashMap<>();
				attrNameMap.put("enName", "sn_enName");
				attrNameMap.put("desc", "sc_desc");
				attrNameMap.put("isRelated", "b_isRelated");
				attrNameMap.put("cnName", "sn_cnName");
				attrNameMap.put("avatarUrl", "sup_avatarUrl");
				attrNameMap.put("authName", "sn_authName");
				attrNameMap.put("gzhUrl", "sup_gzhUrl");
				adapterChain.addLast(new SimpleAttrNameAdapter(attrNameMap));
				
				adapterChain.addLast(new DataListSplitter<Object>());
			}
			
		});
		pipeline.putData(file);
		pipeline.close();
	}
	
}
