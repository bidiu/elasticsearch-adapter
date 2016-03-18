package cn.com.deepdata.es_adapter.adapter;

/**
 * Adapter context, mainly for invoking next {@link Adapter adapter} on the {@link AdapterChain adapter chain}.
 * <p/>
 * This class is thread-safe.
 * 
 * @author sunhe
 * @date 2016年3月18日
 */
public class AdapterContext {
	
	private AdapterChain adapterChain;
	
	private Adapter nextAdapter;
	
	private AdapterContext nextAdapterCtx;

	public synchronized void setAdapterChain(AdapterChain adapterChain) {
		this.adapterChain = adapterChain;
	}

	public synchronized void setNextAdapter(Adapter nextAdapter) {
		this.nextAdapter = nextAdapter;
	}
	
	public synchronized void setNextAdapterCtx(AdapterContext nextAdapterCtx) {
		this.nextAdapterCtx = nextAdapterCtx;
	}
	
	public Object fireNextAdapter(Object data) {
		if (nextAdapter != null) {
			if (adapterChain.isInbound()) {
				return nextAdapter.inboundAdapt(nextAdapterCtx, data);
			}
			else {
				return nextAdapter.outboundAdapt(nextAdapterCtx, data);
			}
		}
		else {
			// current adapter is the last on the chain
			return data;
		}
	}
	
}
