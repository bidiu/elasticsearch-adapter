package cn.com.deepdata.es_adapter.adapter;

import java.io.File;
import java.io.FileInputStream;

/**
 * 适配器，将字符文件（{@link File} or {@link String} (file name)）适配为
 * 一行一行的文本（{@link String} format），可以指定字符文件的编码.
 * <p/>
 * 适配后的每一行文本单独放到数据队列中，所以后面的适配器应可以适配{@link String}类型，
 * 而不是List&ltString&gt类型.
 */
public class File2LinesAdapter extends InputStream2LinesAdapter {
	
	public File2LinesAdapter() {
		// empty
	}
	
	public File2LinesAdapter(String fileEncoding) {
		super(fileEncoding);
	}
	
	@Override
	public Object inboundAdapt(Object data, AdapterContext ctx) throws Exception {
		/*
		 * cast parameter
		 */
		File file = null;
		if (data instanceof File) {
			file = (File) data;
		}
		else if (data instanceof String) {
			file = new File((String) data);
		}
		else {
			throw new IllegalArgumentException("Given data should be either in File, or String type, which is file's name");
		}
		
		return super.inboundAdapt(new FileInputStream(file), ctx);
	}

	@Override
	public Object outboundAdapt(Object data, AdapterContext ctx) throws Exception {
		throw new IllegalStateException("This adapter cannot be used in outbound mode");
	}

}
