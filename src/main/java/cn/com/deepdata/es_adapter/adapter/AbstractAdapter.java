package cn.com.deepdata.es_adapter.adapter;

import java.util.Map;

import cn.com.deepdata.es_adapter.common.ExceptionEvent;

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
	
	@Override
	public final Object inboundAdapt(AdapterContext ctx, Object data) throws Exception {
		try {
			data = inboundAdapt(data, ctx.getMsg());
		}
		catch (Exception e) {
			data = onException(ctx, new ExceptionEvent(e, data));
		}
		return ctx.fireNextAdapter(data);
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
	public final Object outboundAdapt(AdapterContext ctx, Object data) throws Exception {
		try {
			data = outboundAdapt(data, ctx.getMsg());
		}
		catch (Exception e) {
			data = onException(ctx, new ExceptionEvent(e, data));
		}
		return ctx.fireNextAdapter(data);
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
	public final Object onException(AdapterContext ctx, ExceptionEvent event) throws Exception {
		Object result = ctx.isInbound() ? onInboundException(event) : onOutboundException(event);
		
		if (result instanceof ExceptionEvent) {
			event = (ExceptionEvent) result;
			if (event.shouldPropagate()) {
				return ctx.fireNextException(event);
			}
			else {
				throw event.getCause();
			}
		}
		else {
			// recover from exception successfully
			return result;
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
