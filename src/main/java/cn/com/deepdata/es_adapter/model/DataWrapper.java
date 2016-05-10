package cn.com.deepdata.es_adapter.model;

/**
 * Thread-safe.
 * 
 * @author sunhe
 * @date 2016年5月9日
 */
public class DataWrapper {
	
	private Object data;
	
	/**
	 * first processing adapter's class literal
	 */
	private Class<?> firstAdapterClazz;
	
	public DataWrapper(Object data) {
		this(data, null);
	}
	
	public DataWrapper(Object data, Class<?> firstAdapterClazz) {
		this.data = data;
		this.firstAdapterClazz = firstAdapterClazz;
	}

	public synchronized Object getData() {
		return data;
	}
	
	public synchronized void setData(Object data) {
		this.data = data;
	}

	public synchronized Class<?> getFirstAdapterClazz() {
		return firstAdapterClazz;
	}

	public synchronized void setFirstAdapterClazz(Class<?> firstAdapterClazz) {
		this.firstAdapterClazz = firstAdapterClazz;
	}
	
}
