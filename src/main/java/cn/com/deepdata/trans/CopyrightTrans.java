package cn.com.deepdata.trans;

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
import cn.com.deepdata.es_adapter.example.WechatOfficalAccountTrans;
import cn.com.deepdata.es_adapter.listener.DefaultIndexResponseListener;

/**
 * 
 * @author sunhe
 * @date May 25, 2016
 */
public class CopyrightTrans {
	
	public static void copyrightTrans() throws InterruptedException {
		InputStream stream = WechatOfficalAccountTrans.class.getResourceAsStream("/temp/著作权合同备案公告-所有.csv");
		
		PipelineSettings settings = PipelineSettings.builder()
				.host("localhost")
				.port(9300)
				.index("copyright")
				.type("copyright")
				.timeoutAfterClosing(15)
				.build();
		Pipeline pipeline = Pipeline.build(settings, new AdapterChainInitializer() {
			
			@Override
			public void initialize(AdapterChain adapterChain) {
				adapterChain.addLast(new InputStream2LinesAdapter());
				
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
				adapterChain.addLast(new DelimitedString2MapAdapter(titleList, "^"));
			}
			
		}, new DefaultIndexResponseListener() {
			
			AtomicInteger successCnt = new AtomicInteger(0);
			
			@Override
			public void onResponse(IndexResponse response) {
				System.out.println(successCnt.getAndIncrement() + " documents has been indexed.");
			}
			
		});
		
		pipeline.putData(stream);
		pipeline.close();
		System.out.println("All done.");
	}
	
	public static void softwareCopyrightChgTrans() throws InterruptedException {
		InputStream stream = WechatOfficalAccountTrans.class.getResourceAsStream("/temp/计算机软件著作权变更登记公告-所有.csv");
		
		PipelineSettings settings = PipelineSettings.builder()
				.host("localhost")
				.port(9300)
				.index("copyright")
				.type("software_copyright_chg")
				.timeoutAfterClosing(15)
				.build();
		Pipeline pipeline = Pipeline.build(settings, new AdapterChainInitializer() {
			
			@Override
			public void initialize(AdapterChain adapterChain) {
				adapterChain.addLast(new InputStream2LinesAdapter());
				
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
				adapterChain.addLast(new DelimitedString2MapAdapter(titleList, "^"));
			}
			
		}, new DefaultIndexResponseListener() {
			
			AtomicInteger successCnt = new AtomicInteger(0);
			
			@Override
			public void onResponse(IndexResponse response) {
				System.out.println(successCnt.getAndIncrement() + " documents has been indexed.");
			}
			
		});
		
		pipeline.putData(stream);
		pipeline.close();
		System.out.println("All done.");
	}
	
	public static void main(String[] args) throws InterruptedException {
		copyrightTrans();
	}
	
}
