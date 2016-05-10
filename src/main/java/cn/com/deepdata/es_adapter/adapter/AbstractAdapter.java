package cn.com.deepdata.es_adapter.adapter;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import cn.com.deepdata.es_adapter.common.ExceptionEvent;
import cn.com.deepdata.es_adapter.model.DataWrapper;

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
	
	private BlockingQueue<DataWrapper> dataQueue;
	
	protected BlockingQueue<DataWrapper> getDataQueue() {
		if (dataQueue == null) {
			throw new IllegalStateException("You have to register this adapter to an adpter chain first");
		}
		else {
			return dataQueue;
		}
	}
	
	@Override
	public final DataWrapper inboundAdapt(AdapterContext ctx, DataWrapper dataWrapper) throws Exception {
		try {
			Class<?> clazz = dataWrapper.getFirstAdapterClazz();
			if (clazz == null || clazz == this.getClass()) {
				dataWrapper.setFirstAdapterClazz(null);
				Object adaptedData = inboundAdapt(dataWrapper.getData(), ctx.getMsg());
				dataWrapper.setData(adaptedData);
			}
		}
		catch (Exception e) {
			dataWrapper = onException(ctx, new ExceptionEvent(e, dataWrapper));
		}
		return ctx.fireNextAdapter(dataWrapper);
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
	public abstract Object inboundAdapt(Object data, Map<String, Object> msg) throws Exception;
	
	@Override
	public final DataWrapper outboundAdapt(AdapterContext ctx, DataWrapper dataWrapper) throws Exception {
		try {
			Class<?> clazz = dataWrapper.getFirstAdapterClazz();
			if (clazz == null || clazz == this.getClass()) {
				dataWrapper.setFirstAdapterClazz(null);
				Object adaptedData = outboundAdapt(dataWrapper.getData(), ctx.getMsg());
				dataWrapper.setData(adaptedData);
			}
		}
		catch (Exception e) {
			dataWrapper = onException(ctx, new ExceptionEvent(e, dataWrapper));
		}
		return ctx.fireNextAdapter(dataWrapper);
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
	public abstract Object outboundAdapt(Object data, Map<String, Object> msg) throws Exception;
	
	@Override
	public final DataWrapper onException(AdapterContext ctx, ExceptionEvent event) throws Exception {
		Object adaptedData = ctx.isInbound() ? onInboundException(event) : onOutboundException(event);
		
		if (adaptedData instanceof ExceptionEvent) {
			event = (ExceptionEvent) adaptedData;
			if (event.shouldPropagate()) {
				return ctx.fireNextException(event);
			}
			else {
				throw event.getCause();
			}
		}
		else {
			// recover from exception successfully
			DataWrapper dataWrapper = event.getDataWrapper();
			dataWrapper.setData(adaptedData);
			return dataWrapper;
		}
	}
	
	/**
	 * @param event
	 * @return
	 * 		If the exception handler method can recover from the exception, then 
	 * 		the method should return the processed data, otherwise just return the provided 
	 * 		parameter of the method, exception event.
	 * @author sunhe
	 * @date Apr 17, 2016
	 */
	public Object onInboundException(ExceptionEvent event) {
		return event;
	}
	
	/**
	 * @param event
	 * @return
	 * 		If the exception handler method can recover from the exception, then 
	 * 		the method should return the processed data, otherwise just return the provided 
	 * 		parameter of the method, exception event.
	 * @author sunhe
	 * @date Apr 17, 2016
	 */
	public Object onOutboundException(ExceptionEvent event) {
		return event;
	};
	
}
