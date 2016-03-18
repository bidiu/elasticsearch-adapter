package cn.com.deepdata.es_adapter.common;

/**
 * Classes that can be closed to release any underlying resources.
 * <p/>
 * 
 * @author sunhe
 * @date Mar 18, 2016
 */
public interface Closeable {
	
	/**
	 * Repetitive invocation of this method has no 
	 * side effect.
	 * 
	 * @author sunhe
	 * @date Mar 18, 2016
	 */
	public void close();
	
}
