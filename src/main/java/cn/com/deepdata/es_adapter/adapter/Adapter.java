package cn.com.deepdata.es_adapter.adapter;

/**
 * Adapt one specific type of data to another.
 * <p/>
 * Implementation of this interface MUST be thread-safe.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public interface Adapter {
	
	/**
	 * Note that parameter 'ctx' MUST NOT be altered in this method.
	 * 
	 * @param ctx
	 * @param data
	 * @return
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public Object inboundAdapt(AdapterContext ctx, Object data);
	
	/**
	 * Note that parameter 'ctx' MUST NOT be altered in this method.
	 * 
	 * @param ctx
	 * @param data
	 * @return
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public Object outboundAdapt(AdapterContext ctx, Object data);
	
}
