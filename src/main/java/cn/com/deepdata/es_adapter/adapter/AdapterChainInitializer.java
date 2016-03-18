package cn.com.deepdata.es_adapter.adapter;

/**
 * User can implement this interface to initialize an {@link AdapterChain adapter chain}.
 * <p/>
 * Note that no matter whether the adapter chain is inbound or outbound, 
 * the first adapter of the chain will be invoked first, then following 
 * adapters on the adapter chain will be invoked sequentially one by one.
 * <p/>
 * And also note that implementation of this interface should be thread-safe.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public interface AdapterChainInitializer {
	
	/**
	 * Invoke the parameter "adapterChian" 's method {@link AdapterChain#addLast(Adapter)} 
	 * to add adapters.
	 * 
	 * @param adapterChain
	 * @author sunhe
	 * @date Mar 18, 2016
	 */
	public void initialize(AdapterChain adapterChain);
	
}
