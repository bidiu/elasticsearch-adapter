package cn.com.deepdata.es_adapter.adapter;

import java.util.concurrent.BlockingQueue;

import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.SkipAdaptingException;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * Abstract adapter class for extension. You should only 
 * override the abstract methods of this class.
 * <p/>
 * And note that extension of this class MUST be thread-safe.
 * <p/>
 * Also see {@link Adapter}.
 */
public abstract class AbstractAdapter implements Adapter {
	
	private BlockingQueue<DataWrapper> dataQueue;
	
	/**
	 * Get the blocking queue that the elasticsearch adapter 
	 * library uses internally. You can then put data with it.
	 * <p/>
	 * Note that it's recommended to invoke {@link #putData(Object)} 
	 * or {@link #putData(Object, Class)}, instead of this one.
	 * 
	 * @return
	 * 		the blocking queue that the elasticsearch adapter library uses
	 */
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
	 * Put data into the blocking queue that the elasticsearch adapter 
	 * library uses internally. The put data will be first adapted by the 
	 * first adapter in the chain, and then subsequent adapters one by 
	 * one.
	 * 
	 * @param data
	 * 		data going to be put into the queue
	 * @throws InterruptedException
	 */
	protected final void putData(Object data) throws InterruptedException {
		getDataQueue().put(new DataWrapper(data));
	}
	
	/**
	 * Put data into the blocking queue that the elasticsearch adapter 
	 * library uses internally. The put data will be first adapted by the 
	 * adapter that assigned by the given {@link Class} parameter, and then 
	 * subsequent adapters one by one.
	 * 
	 * @param data
	 * 		data going to be put into the queue
	 * @param firstAdapterClazz
	 * 		the first adapter's class literal that will first adapt the given data.
	 * @throws InterruptedException
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
	 * @param data
	 * 		data to be processed
	 * @param msg
	 * 		current adapter's custom message
	 * @return
	 * 		the resulting data
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
	 * @param data
	 * 		data to be processed
	 * @param msg
	 * 		current adapter's custom message
	 * @return
	 * 		the resulting data
	 */
	public abstract Object outboundAdapt(Object data, AdapterContext ctx) throws Exception;
	
	@Override
	public final DataWrapper onException(AdapterContext ctx, ExceptionEvent event) throws Exception {
		if (event.getCause() instanceof SkipAdaptingException) {
			event.setShouldPropagate(false);
			throw event.getCause();
		}
		
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
	 * 		parameter {@link ExceptionEvent}.
	 */
	public Object onInboundException(ExceptionEvent event) {
		return event;
	}
	
	/**
	 * @param event
	 * @return
	 * 		If the exception handler method can recover from the exception, then 
	 * 		the method should return the processed data, otherwise just return the provided 
	 * 		parameter {@link ExceptionEvent}.
	 */
	public Object onOutboundException(ExceptionEvent event) {
		return event;
	};
	
}
