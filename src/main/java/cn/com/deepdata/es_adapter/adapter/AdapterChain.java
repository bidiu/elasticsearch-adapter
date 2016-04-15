package cn.com.deepdata.es_adapter.adapter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The adapter chain composed of several adapters and its meta data.
 * <p/>
 * This class is thread-safe.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class AdapterChain {
	
	private boolean isInbound;
	
	private Adapter firstAdapter;
	
	private List<AdapterContext> adapterCtxList;

	public synchronized boolean isInbound() {
		return isInbound;
	}
	
	public AdapterChain(boolean isInbound) {
		this.isInbound = isInbound;
		firstAdapter = null;
		adapterCtxList = new LinkedList<AdapterContext>();
	}
	
	/**
	 * Add a adapter to the rear of adapter chain
	 * 
	 * @param adapter
	 * @return
	 * 		the adapter chain itself
	 * @author sunhe
	 * @date Mar 18, 2016
	 */
	public synchronized AdapterChain addLast(Adapter adapter) {
		return addLast(adapter, new HashMap<String, Object>());
	}
	
	/**
	 * 
	 * @param adapter
	 * @param msg
	 * @return
	 * @author sunhe
	 * @date 2016年4月5日
	 */
	public synchronized AdapterChain addLast(Adapter adapter, Map<String, Object> msg) {
		AdapterContext adapterCtx = new AdapterContext();
		adapterCtx.setMsg(msg);
		
		if (adapterCtxList.size() == 0) {
			firstAdapter = adapter;
		}
		else if (adapterCtxList.size() > 0) {
			// link current rear adapter context to current added one
			AdapterContext lastAdapterCtx = adapterCtxList.get(adapterCtxList.size() - 1);
			lastAdapterCtx.setNextAdapterCtx(adapterCtx);
			lastAdapterCtx.setNextAdapter(adapter);
		}
		
		adapterCtx.setAdapterChain(this);
		adapterCtx.setNextAdapterCtx(null);
		adapterCtx.setNextAdapter(null);
		adapterCtxList.add(adapterCtx);
		return this;
	}
	
	/**
	 * Fire the first adapter in the chain, all following adapters
	 * on this chain will be fired sequentially one by one.
	 * 
	 * @param data
	 * 		data to be processed
	 * @return
	 * 		the resulting data
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public Object fireAdapters(Object data) {
		if (firstAdapter == null) {
			// not any adapter available on the chain
			return data;
		}
		else if (isInbound) {
			return firstAdapter.inboundAdapt(adapterCtxList.get(0), data);
		}
		else {
			return firstAdapter.outboundAdapt(adapterCtxList.get(0), data);
		}
	}
	
}
