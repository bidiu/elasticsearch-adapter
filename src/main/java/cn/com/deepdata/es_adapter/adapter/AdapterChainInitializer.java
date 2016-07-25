package cn.com.deepdata.es_adapter.adapter;

/**
 * 你可以实现这个接口来实例化一个适配器链{@link AdapterChain}.
 * <p/>
 * 不管是inbound模式还是outbound模式(目前不支持)，最先被处罚的适配器都是适配器链上的第一个适配器.
 * <p/>
 * And also note that implementation of this interface should be thread-safe.
 */
public interface AdapterChainInitializer {
	
	/**
	 * 调用参数{@link AdapterChain}的{@link AdapterChain#addLast(Adapter)}或
	 * {@link AdapterChain#addLast(Adapter, java.util.Map)}方法来向适配器链
	 * 添加适配器.
	 * 
	 * @param adapterChain
	 */
	public void initialize(AdapterChain adapterChain);
	
}
