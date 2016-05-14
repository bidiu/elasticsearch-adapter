package cn.com.deepdata.es_adapter.adapter;

/**
 * User can implement this interface to initialize an {@link AdapterChain adapter chain}.
 * <p/>
 * Note that no matter whether the adapter chain is inbound or outbound, 
 * the first adapter of the chain will be invoked first, then following 
 * adapters on the adapter chain will be invoked sequentially one by one.
 * <p/>
 * And also note that implementation of this interface should be thread-safe.
 */
public interface AdapterChainInitializer {
	
	/**
	 * Invoke the parameter "adapterChian" 's method {@link AdapterChain#addLast(Adapter)} 
	 * or {@link AdapterChain#addLast(Adapter, java.util.Map)}
	 * to add adapters.
	 * 
	 * @param adapterChain
	 */
	public void initialize(AdapterChain adapterChain);
	
}
