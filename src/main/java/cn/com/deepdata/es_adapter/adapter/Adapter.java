package cn.com.deepdata.es_adapter.adapter;

import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * Adapt one specific type of data to another, or from a
 * one specific type to the same, but with some modification.
 * <p/>
 * Note that implementation of this interface MUST be thread-safe.
 * <p/>
 * You MUST extend {@link AbstractAdapter}, instead of 
 * implementing this interface completely by your own.
 */
public interface Adapter {
	
	/**
	 * Note that parameter "ctx" MUST NOT be altered in this method, 
	 * or the outcome is undefined.
	 * 
	 * @param ctx
	 * @param dataWrapper
	 * 		the data to be processed
	 * @return
	 * 		the resulting data wrapper
	 */
	public DataWrapper inboundAdapt(AdapterContext ctx, DataWrapper dataWrapper) throws Exception;
	
	/**
	 * Note that parameter "ctx" MUST NOT be altered in this method, 
	 * or the outcome is undefined.
	 * 
	 * @param ctx
	 * @param dataWrapper
	 * 		the data to be processed
	 * @return
	 * 		the resulting data wrapper
	 */
	public DataWrapper outboundAdapt(AdapterContext ctx, DataWrapper dataWrapper) throws Exception;
	
	/**
	 * This method will be called when exception occurs during the 
	 * execution of {@link #inboundAdapt(AdapterContext, DataWrapper)} 
	 * and {@link #outboundAdapt(AdapterContext, DataWrapper)}.
	 * 
	 * @param ctx
	 * @param event
	 * @return
	 * 		the resulting data wrapper
	 * @throws Exception
	 */
	public DataWrapper onException(AdapterContext ctx, ExceptionEvent event) throws Exception;
	
}
