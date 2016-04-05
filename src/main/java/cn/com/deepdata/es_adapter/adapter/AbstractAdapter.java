package cn.com.deepdata.es_adapter.adapter;

import java.util.Map;

/**
 * Abstract adapter class for extension. It's highly recommended to only 
 * override the abstract methods of this class.
 * <p/>
 * And note that extension of this class MUST be thread-safe.
 * <p/>
 * Also see {@link Adapter}.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public abstract class AbstractAdapter implements Adapter {
	
	@Override
	public Object inboundAdapt(AdapterContext ctx, Object data) {
		return ctx.fireNextAdapter(inboundAdapt(data, ctx.getMsg()));
	}
	
	/**
	 * 
	 * @param data
	 * 		data to be processed
	 * @param msg
	 * 		current adapter's custom message
	 * @return
	 * 		the resulting data
	 * @author sunhe
	 * @date 2016年4月5日
	 */
	public abstract Object inboundAdapt(Object data, Map<String, Object> msg);
	
	@Override
	public Object outboundAdapt(AdapterContext ctx, Object data) {
		return ctx.fireNextAdapter(outboundAdapt(data, ctx.getMsg()));
	}
	
	/**
	 * 
	 * @param data
	 * 		data to be processed
	 * @param msg
	 * 		current adapter's custom message
	 * @return
	 * 		the resulting data
	 * @author sunhe
	 * @date 2016年4月5日
	 */
	public abstract Object outboundAdapt(Object data, Map<String, Object> msg);
	
}
