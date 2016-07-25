package cn.com.deepdata.es_adapter.example;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.index.IndexResponse;

import cn.com.deepdata.es_adapter.Pipeline;
import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.adapter.DelimitedString2MapAdapter;
import cn.com.deepdata.es_adapter.adapter.InputStream2LinesAdapter;
import cn.com.deepdata.es_adapter.listener.DefaultIndexResponseListener;

/**
 * example for CSV file
 * <p/>
 * Also see {@link WechatOfficalAccountTrans} for example of JSON list file.
 * 
 * @author sunhe
 * @date May 25, 2016
 */
public class CopyrightTrans {
	
	public static void copyrightTrans() throws InterruptedException {
		// 要适配的数据，是一个CSV文件
		InputStream stream = WechatOfficalAccountTrans.class.getResourceAsStream("/example/著作权合同备案公告-所有.csv");
		
		// 实例化一个settings
		PipelineSettings settings = PipelineSettings.builder()
				.host("localhost")
				.port(9200)
				.index("flume-copyright")
				.type("flumetype")
				.timeoutAfterClosing(3)
				.build();
		// 根据settings，实例化一个pipeline
		Pipeline pipeline = Pipeline.build(settings, new AdapterChainInitializer() {
			
			// 这个方法拼装适配器链
			@Override
			public void initialize(AdapterChain adapterChain) {
				// 添加第一个适配器，可以将InputStream（CSV文件）适配为若干个单独的字符串（以换行符分割）
				adapterChain.addLast(new InputStream2LinesAdapter());
				
				// 以下是第二个适配器
				List<String> titleList = new ArrayList<String>();
				titleList.add("sn_caseCode");
				titleList.add("td_caseDate");
				titleList.add("sc_workName");
				titleList.add("sn_workType");
				titleList.add("sc_assignor");
				titleList.add("sc_assignee");
				titleList.add("sc_rightScope");
				titleList.add("sc_location");
				titleList.add("sn_contractPeriod");
				// 添加第二个适配器，将'^'字符分割的字符序列（第一个适配器的输出）适配为JSON(Map表示)
				adapterChain.addLast(new DelimitedString2MapAdapter(titleList, "\\^"));
				
				// 不需要再处理了，数据一旦被适配为byte[]，String，Map或XContentBuilder表示的合法JSON，
				// 就可以被入库了.
			}
		
		// 添加一个ElasticSearch请求响应的监听
		}, new DefaultIndexResponseListener() {
			
			AtomicInteger successCnt = new AtomicInteger(0);
			
			// 没有发生异常时被回调
			@Override
			public void onResponse(IndexResponse response) {
				// 这里之做简单的计数
				System.out.println(successCnt.getAndIncrement() + " documents has been indexed.");
			}
			
		});
		
		// 提供CSV数据，开始适配，异步执行的
		pipeline.putData(stream);
		// 同步阻塞，关闭pipeline（必须有这步）
		pipeline.close();
		// 提示结束（建议包括）
		System.out.println("All done.");
	}
	
	public static void main(String[] args) throws InterruptedException {
		copyrightTrans();
	}
	
}
