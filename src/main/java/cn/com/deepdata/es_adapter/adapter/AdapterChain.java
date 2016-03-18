package cn.com.deepdata.es_adapter.adapter;

import java.util.LinkedList;
import java.util.List;

/**
 * Thread-safe.
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
	
	public synchronized AdapterChain addLast(Adapter adapter) {
		AdapterContext adapterCtx = new AdapterContext();
		
		// link previous adapter context to current one
		if (adapterCtxList.size() > 0) {
			firstAdapter = adapter;
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
	 * @return
	 * @author sunhe
	 * @date 2016年3月18日
	 */
	public Object fireFirstAdapter(Object data) {
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
