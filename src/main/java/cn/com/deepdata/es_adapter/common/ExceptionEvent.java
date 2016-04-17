package cn.com.deepdata.es_adapter.common;

/**
 * This class is not thread-safe.
 * 
 * @author sunhe
 * @date Apr 17, 2016
 */
public class ExceptionEvent {
	
	private Exception cause;
	
	private Object data;
	
	private boolean shouldPropagate;
	
	public ExceptionEvent(Exception cause, Object data) {
		this.cause = cause;
		this.data = data;
		shouldPropagate = true;
	}

	public Exception getCause() {
		return cause;
	}

	public Object getData() {
		return data;
	}

	public boolean shouldPropagate() {
		return shouldPropagate;
	}

	public void setPropagate(boolean shouldPropagate) {
		this.shouldPropagate = shouldPropagate;
	}

}
