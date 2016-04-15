package cn.com.deepdata.es_adapter.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;

import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings;
import cn.com.deepdata.es_adapter.Pipeline.PipelineSettings.SettingsBuilder;
import cn.com.deepdata.es_adapter.adapter.Adapter;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.adapter.AdapterChainInitializer;
import cn.com.deepdata.es_adapter.listener.DefaultIndexResponseListener;
import cn.com.deepdata.es_adapter.util.CsvFileProcessor;
import cn.com.deepdata.es_adapter.util.FilenameUtil;

/**
 * 
 * @author sunhe
 * @date 2016年4月5日
 */
public class SoftwareCopyrightTransfer {
	
	public static final String ROOT_DIR = "/home/hadoop/software-copyright-data";
	public static final String HOST = "localhost";
	public static final int PORT = 19300;
	public static final String INDEX_NAME = "copyright";
	public static final String TYPE_NAME = "software_copyright";
	public static final int THREAD_POOL_SIZE = 8;
	
	/**
	 * 抽取省或直辖市
	 * 
	 * @param filename
	 * @return
	 * @author sunhe
	 * @date 2016年4月14日
	 */
	private static String extractProvince(String filename) {
		filename = FilenameUtil.excludeExtensionName(filename);
		if (filename.indexOf("-") == filename.lastIndexOf("-")) {
			return filename.substring(filename.lastIndexOf("-") + 1);
		}
		else {
			return filename.substring(filename.indexOf("-") + 1, filename.lastIndexOf("-"));
		}
	}
	
	/**
	 * 抽取区或省市
	 * 
	 * @param filename
	 * @return
	 * @author sunhe
	 * @date 2016年4月14日
	 */
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
		final Adapter adapter = new ChinaSoftwareCopyrightLocationAdapter();
		
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
			// log
			System.out.println("Processing file " + file.getPath());
			
			// prepare message - province and/or city
			final Map<String, Object> msg = new HashMap<String, Object>();
			msg.put("province", extractProvince(file.getName()));
			msg.put("city", extractCity(file.getName()));
			
			// instantiate builder
			SettingsBuilder builder = PipelineSettings.builder()
					.index(INDEX_NAME)
					.type(TYPE_NAME)
					.threadPoolSize(THREAD_POOL_SIZE)
					.host(HOST)
					.port(PORT);
			
			// process the file
			new CsvFileProcessor(file, "UTF-8", ",", false, 
					builder, true, false, new AdapterChainInitializer() {
						
						@Override
						public void initialize(AdapterChain adapterChain) {
							adapterChain.addLast(adapter, msg);
						}
					}, new DefaultIndexResponseListener())
					.process();
		}
	}
	
}
