package cn.com.deepdata.es_adapter.adapter;

import cn.com.deepdata.es_adapter.Pipeline;

/**
 * Adapt one specific type of data to another, or from a
 * one specific type to the same, but with some modification.
 * <p/>
 * Note that implementation of this interface MUST be thread-safe, if you 
 * plan to share it across multiple {@link Pipeline}s.
 * <p/>
 * Typically, it's HIGHLY recommended to extend {@link AbstractAdapter}, instead of 
 * implementing this interface completely by your own.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public interface Adapter {
	
	/**
	 * Note that parameter "ctx" MUST NOT be altered in this method, 
	 * or the outcome is undefined.
	 * <p/>
	 * TODO except the msg
	 * 
	 * @param ctx
	 * @param data
	 * 		the data to be processed
	 * @return
	 * 		the resulting data
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public Object inboundAdapt(AdapterContext ctx, Object data);
	
	/**
	 * Note that parameter "ctx" MUST NOT be altered in this method, 
	 * or the outcome is undefined.
	 * <p/>
	 * TODO except the msg
	 * 
	 * 
	 * @param ctx
	 * @param data
	 * 		the data to be processed
	 * @return
	 * 		the resulting data
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public Object outboundAdapt(AdapterContext ctx, Object data);
	
}
