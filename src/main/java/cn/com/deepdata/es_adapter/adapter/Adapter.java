package cn.com.deepdata.es_adapter.adapter;

import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.model.DataWrapper;

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
	 * @param dataWrapper
	 * 		the data to be processed
	 * @return
	 * 		the resulting data wrapper
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public DataWrapper inboundAdapt(AdapterContext ctx, DataWrapper dataWrapper) throws Exception;
	
	/**
	 * Note that parameter "ctx" MUST NOT be altered in this method, 
	 * or the outcome is undefined.
	 * 
	 * 
	 * @param ctx
	 * @param dataWrapper
	 * 		the data to be processed
	 * @return
	 * 		the resulting data wrapper
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public DataWrapper outboundAdapt(AdapterContext ctx, DataWrapper dataWrapper) throws Exception;
	
	/**
	 * Called when exception occurs.
	 * 
	 * @param ctx
	 * @param e
	 * @return
	 * 		the resulting data wrapper
	 * @throws Exception
	 * @author sunhe
	 * @date Apr 17, 2016
	 */
	public DataWrapper onException(AdapterContext ctx, ExceptionEvent event) throws Exception;
	
}
