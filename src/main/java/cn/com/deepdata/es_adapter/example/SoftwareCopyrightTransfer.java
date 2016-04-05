package cn.com.deepdata.es_adapter.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.adapter.Adapter;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.util.CsvFileProcessor;
import cn.com.deepdata.es_adapter.util.FilenameUtil;

/**
 * 
 * @author sunhe
 * @date 2016年4月5日
 */
public class SoftwareCopyrightTransfer {
	
	public static final String ROOT_DIR = "C:\\Users\\deepdata\\Downloads\\temp\\著作权采集\\previous\\著作权数据-20160301\\software";
	
	private static String extractProvince(String filename) {
		filename = FilenameUtil.excludeExtensionName(filename);
		if (filename.indexOf("-") == filename.lastIndexOf("-")) {
			return filename.substring(filename.lastIndexOf("-") + 1);
		}
		else {
			return filename.substring(filename.indexOf("-") + 1, filename.lastIndexOf("-"));
		}
	}
	
	private static String extractCity(String filename) {
		filename = FilenameUtil.excludeExtensionName(filename);
		if (filename.indexOf("-") == filename.lastIndexOf("-")) {
			return null;
		}
		else {
			return filename.substring(filename.lastIndexOf("-") + 1);
		}
	}
	
	public static void main(String[] args) throws FileNotFoundException, InterruptedException {
		// instantiate adapter
		final Adapter adapter = new SoftwareCopyrightLocationAdapter();
		// filter all CSV files
		File[] files = new File(ROOT_DIR).listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				if ("csv".equals(FilenameUtil.getExtensionName(name))) {
					return true;
				}
				else {
					return false;
				}
			}
			
		});
		for (File file : files) {
			// prepare message - province and/or city
			final Map<String, Object> msg = new HashMap<String, Object>();
			msg.put("country", extractProvince(file.getName()));
			msg.put("city", extractCity(file.getName()));
			// add adapter
			PipelineSettings settings = PipelineSettings.getDefaultSettings()
					.index("index-test")
					.type("type-test")
					.threadPoolSize(1)
					.adapterChainInitializer(new AdapterChainInitializer() {
						
						@Override
						public void initialize(AdapterChain adapterChain) {
							adapterChain.addLast(adapter, msg);
						}
						
					});
			// process the file
			new CsvFileProcessor(file, "UTF-8", ",", false, settings, true, false).process();
		}
	}
	
}
