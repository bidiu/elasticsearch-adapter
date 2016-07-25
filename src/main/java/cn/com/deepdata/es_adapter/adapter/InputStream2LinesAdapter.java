package cn.com.deepdata.es_adapter.adapter;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import cn.com.deepdata.es_adapter.SkipAdaptingException;

/**
 * 适配器，将字符流{@link InputStream}适配为一行一行的字符串.
 * <p/>
 * Also see {@link File2LinesAdapter}.
 * <p/>
 * There is no need to close the input stream by yourself, after draining out the 
 * input stream, this adapter will close it automatically.
 */
public class InputStream2LinesAdapter extends AbstractAdapter implements QueueDataProvidingAdapter {
	
	protected static final String DEFAULT_FILE_ENCODING = "UTF-8";
	
	protected final String fileEncoding;
	
	public InputStream2LinesAdapter() {
		this(DEFAULT_FILE_ENCODING);
	}
	
	public InputStream2LinesAdapter(String fileEncoding) {
		this.fileEncoding = fileEncoding;
	}
	
	@Override
	public Object inboundAdapt(Object data, AdapterContext ctx) throws Exception {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(
					new InputStreamReader(
					(InputStream) data, Charset.forName(fileEncoding)));
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
