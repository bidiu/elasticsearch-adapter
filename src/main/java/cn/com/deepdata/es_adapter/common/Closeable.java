package cn.com.deepdata.es_adapter.common;

/**
 * Classes that can be closed to release underlying resources.
 */
public interface Closeable {
	
	/**
	 * Repetitive invocation of this method should has no 
	 * side effect.
	 */
	public void close();
	
}
