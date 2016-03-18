package cn.com.deepdata.es_adapter.adapter;

/**
 * Whether the adapter chain is inbound or outbound, the first 
 * adapter of the chain will be invoked first, then following 
 * adapters will be invoked sequentially one by one.
 * 
 * Implementation of this interface should be thread-safe.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public interface AdapterChainInitializer {
	
	public void initialize(AdapterChain adapterChain);
	
}
