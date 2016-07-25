package cn.com.deepdata.es_adapter.adapter;

import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * 适配器，将输入的数据适配，并输出
 * <p/>
 * Note that implementation of this interface MUST be thread-safe.
 * <p/>
 * 你不应该直接实现这个接口，而应该继承{@link AdapterChain}.
 */
public interface Adapter {
	
	/**
	 * 注意实现一定不能修改参数{@link AdapterContext}的状态，否则结果是未定义的.
	 * 
	 * @param ctx
	 * @param dataWrapper
	 * 		the data to be processed
	 * @return
	 * 		the resulting data wrapper
	 */
	public DataWrapper inboundAdapt(AdapterContext ctx, DataWrapper dataWrapper) throws Exception;
	
	/**
	 * 注意实现一定不能修改参数{@link AdapterContext}的状态，否则结果是未定义的.
	 * 
	 * @param ctx
	 * @param dataWrapper
	 * 		the data to be processed
	 * @return
	 * 		the resulting data wrapper
	 */
	public DataWrapper outboundAdapt(AdapterContext ctx, DataWrapper dataWrapper) throws Exception;
	
	/**
	 * 如果执行{@link #inboundAdapt(AdapterContext, DataWrapper)}和
	 * {@link #outboundAdapt(AdapterContext, DataWrapper)}时抛出了异常，
	 * 则会执行这个方法.
	 * 
	 * @param ctx
	 * @param event
	 * @return
	 * 		the resulting data wrapper
	 * @throws Exception
	 */
	public DataWrapper onException(AdapterContext ctx, ExceptionEvent event) throws Exception;
	
}
