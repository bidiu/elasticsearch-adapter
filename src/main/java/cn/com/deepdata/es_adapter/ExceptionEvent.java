package cn.com.deepdata.es_adapter;

import cn.com.deepdata.es_adapter.adapter.AbstractAdapter;
import cn.com.deepdata.es_adapter.adapter.Adapter;
import cn.com.deepdata.es_adapter.adapter.AdapterChain;
import cn.com.deepdata.es_adapter.listener.ResponseListener;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * 对适配器{@link Adapter}解析时抛出的（没有catch的）异常的封装.
 * <p/>
 * 默认情况下{@link ExceptionEvent}会沿着适配器链{@link AdapterChain}传播 -- 
 * 如果某个Adapter的{@link AbstractAdapter#onInboundException(ExceptionEvent)}和
 * {@link AbstractAdapter#onOutboundException(ExceptionEvent)}没有从异常中恢复，
 * 则异常会传播到下一个适配器，如果适配器链中的最后一个适配器也没能处理异常，则异常会传递给
 * {@link ResponseListener}.
 * <p/>
 * 你可以在上面提到的两个方法中调用{@link #setShouldPropagate(boolean)}来禁用这种行为.
 * <p/>
 * This class is not thread-safe.
 */
public class ExceptionEvent {
	
	private Exception cause;
	
	private DataWrapper dataWrapper;
	
	private boolean shouldPropagate;
	
	public ExceptionEvent(Exception cause, DataWrapper dataWrapper) {
		this(cause, dataWrapper, true);
	}
	
	public ExceptionEvent(Exception cause, DataWrapper dataWrapper, boolean shouldPropagate) {
		this.cause = cause;
		this.dataWrapper = dataWrapper;
		this.shouldPropagate = shouldPropagate;
	}

	public Exception getCause() {
		return cause;
	}

	public DataWrapper getDataWrapper() {
		return dataWrapper;
	}
	
	public Object getData() {
		return dataWrapper.getData();
	}

	public boolean shouldPropagate() {
		return shouldPropagate;
	}
	
	public void setShouldPropagate(boolean shouldPropagate) {
		this.shouldPropagate = shouldPropagate;
	}

}
