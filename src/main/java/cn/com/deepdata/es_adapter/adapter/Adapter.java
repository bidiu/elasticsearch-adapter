package cn.com.deepdata.es_adapter.adapter;

import cn.com.deepdata.es_adapter.common.ExceptionEvent;

/**
 * Adapt one specific type of data to another, or from a
 * one specific type to the same, but with some modification.
 * <p/>
 * Note that implementation of this interface MUST be thread-safe.
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
	 * 
	 * @param ctx
	 * @param data
	 * 		the data to be processed
	 * @return
	 * 		the resulting data
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public Object inboundAdapt(AdapterContext ctx, Object data) throws Exception;
	
	/**
	 * Note that parameter "ctx" MUST NOT be altered in this method, 
	 * or the outcome is undefined.
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
	public Object outboundAdapt(AdapterContext ctx, Object data) throws Exception;
	
	/**
	 * Called when exception occurs.
	 * 
	 * @param ctx
	 * @param e
	 * @return
	 * 		the resulting data
	 * @throws Exception
	 * @author sunhe
	 * @date Apr 17, 2016
	 */
	public Object onException(AdapterContext ctx, ExceptionEvent event) throws Exception;
	
}
