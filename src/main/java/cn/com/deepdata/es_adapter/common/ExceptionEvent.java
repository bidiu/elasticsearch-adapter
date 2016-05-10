package cn.com.deepdata.es_adapter.common;

import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * This class is not thread-safe.
 * 
 * @author sunhe
 * @date Apr 17, 2016
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
