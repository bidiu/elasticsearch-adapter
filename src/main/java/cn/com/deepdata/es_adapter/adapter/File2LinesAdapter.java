package cn.com.deepdata.es_adapter.adapter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import cn.com.deepdata.es_adapter.SkipAdaptingException;

/**
 * TODO inputstream support
 * 
 * @author sunhe
 * @date May 13, 2016
 */
public class File2LinesAdapter extends AbstractAdapter implements QueueDataProvidingAdapter {
	
	private static final String DEFAULT_FILE_ENCODING = "UTF-8";
	
	private final String fileEncoding;
	
	public File2LinesAdapter() {
		this(DEFAULT_FILE_ENCODING);
	}
	
	public File2LinesAdapter(String fileEncoding) {
		this.fileEncoding = fileEncoding;
	}
	
	@Override
	public Object inboundAdapt(Object data, AdapterContext ctx) throws Exception {
		/*
		 * check parameters
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
		
		/*
		 * read file
		 */
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(
					new InputStreamReader(
					new FileInputStream(file), Charset.forName(fileEncoding)));
			String line = null;
			while ((line = reader.readLine()) != null) {
				putData(line, ctx.getNextAdapterClazz());
			}
			throw new SkipAdaptingException();
		}
		finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	@Override
	public Object outboundAdapt(Object data, AdapterContext ctx) throws Exception {
		throw new IllegalStateException("This adapter cannot be used in outbound mode");
	}

}
