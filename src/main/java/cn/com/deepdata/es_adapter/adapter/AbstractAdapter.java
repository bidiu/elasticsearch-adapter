package cn.com.deepdata.es_adapter.adapter;

import java.util.concurrent.BlockingQueue;

import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.SkipAdaptingException;
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
	
	protected final BlockingQueue<DataWrapper> getDataQueue() {
		if (! (this instanceof QueueDataProvidingAdapter)) {
			throw new IllegalStateException("In order to call this function, the adapter has to implement QueueDataProvidingAdater interface");
		}
		else if (dataQueue == null) {
			throw new IllegalStateException("You have to register this adapter to an adpter chain first");
		}
		else {
			return dataQueue;
		}
	}
	
	/**
	 * TODO for now, this function can be called in both inbound and outbound mode
	 * 
	 * @param data
	 * @throws InterruptedException
	 * @author sunhe
	 * @date 2016年5月12日
	 */
	protected final void putData(Object data) throws InterruptedException {
		getDataQueue().put(new DataWrapper(data));
	}
	
	/**
	 * TODO for now, this function can be called in both inbound and outbound mode
	 * 
	 * @param data
	 * @param firstAdapterClazz
	 * @throws InterruptedException
	 * @author sunhe
	 * @date 2016年5月12日
	 */
	protected final void putData(Object data, Class<?> firstAdapterClazz) throws InterruptedException {
		getDataQueue().put(new DataWrapper(data, firstAdapterClazz));
	}
	
	@Override
	public final DataWrapper inboundAdapt(AdapterContext ctx, DataWrapper dataWrapper) throws Exception {
		try {
			Class<?> clazz = dataWrapper.getFirstAdapterClazz();
			if (clazz == null || clazz == this.getClass()) {
				dataWrapper.setFirstAdapterClazz(null);
				Object adaptedData = inboundAdapt(dataWrapper.getData(), ctx);
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
	public abstract Object inboundAdapt(Object data, AdapterContext ctx) throws Exception;
	
	@Override
	public final DataWrapper outboundAdapt(AdapterContext ctx, DataWrapper dataWrapper) throws Exception {
		try {
			Class<?> clazz = dataWrapper.getFirstAdapterClazz();
			if (clazz == null || clazz == this.getClass()) {
				dataWrapper.setFirstAdapterClazz(null);
				Object adaptedData = outboundAdapt(dataWrapper.getData(), ctx);
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
	public abstract Object outboundAdapt(Object data, AdapterContext ctx) throws Exception;
	
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
		if (event.getCause() instanceof SkipAdaptingException) {
			event.setShouldPropagate(false);
		}
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
		if (event.getCause() instanceof SkipAdaptingException) {
			event.setShouldPropagate(false);
		}
		return event;
	};
	
}
