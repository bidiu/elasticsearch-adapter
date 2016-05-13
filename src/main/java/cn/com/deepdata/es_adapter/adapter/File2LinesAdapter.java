package cn.com.deepdata.es_adapter.adapter;

import java.io.File;
import java.io.FileInputStream;

/**
 * @author sunhe
 * @date May 13, 2016
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
