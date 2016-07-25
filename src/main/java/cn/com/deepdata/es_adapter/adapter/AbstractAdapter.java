package cn.com.deepdata.es_adapter.adapter;

import java.util.concurrent.BlockingQueue;

import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.SkipAdaptingException;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * 适配器的抽象基类，你应该继承这个类，并实现抽象方法，而不是直接实现接口
 * {@link Adapter}.
 * <p/>
 * And note that extension of this class MUST be thread-safe.
 * <p/>
 * Also see {@link Adapter}.
 */
public abstract class AbstractAdapter implements Adapter {
	
	private BlockingQueue<DataWrapper> dataQueue;
	
	/**
	 * 得到内部是用的数据阻塞队列，你可以向其动态地添加数据.
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
	 * 向内部使用的阻塞队列添加数据. 被添加的数据将首先被适配器链的第一个适配器适配，
	 * 然后一个接着一个.
	 * 
	 * @param data
	 * 		data going to be put into the queue
	 * @throws InterruptedException
	 */
	protected final void putData(Object data) throws InterruptedException {
		getDataQueue().put(new DataWrapper(data));
	}
	
	/**
	 * 向内部使用的阻塞队列添加数据. 被添加的数据首先会被{@link Class}指定的适配器适配，
	 * 然后一个接着一个.
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
	 * See {@link ExceptionEvent}.
	 * 
	 * @param event
	 * @return
	 * 		如何异常处理器（即本方法）可以从异常中恢复，则返回适配后的数据，
	 * 		否则直接返回参数{@link ExceptionEvent}.
	 */
	public Object onInboundException(ExceptionEvent event) {
		return event;
	}
	
	/**
	 * See {@link ExceptionEvent}.
	 * 
	 * @param event
	 * @return
	 * 		如何异常处理器（即本方法）可以从异常中恢复，则返回适配后的数据，
	 * 		否则直接返回参数{@link ExceptionEvent}.
	 */
	public Object onOutboundException(ExceptionEvent event) {
		return event;
	};
	
}
