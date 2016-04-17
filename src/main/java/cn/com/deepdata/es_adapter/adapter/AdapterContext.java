package cn.com.deepdata.es_adapter.adapter;

import java.util.Map;

import cn.com.deepdata.es_adapter.common.ExceptionEvent;

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
	
	private Map<String, Object> msg;
	
	public synchronized boolean isInbound() {
		return adapterChain.isInbound();
	}

	public synchronized void setAdapterChain(AdapterChain adapterChain) {
		this.adapterChain = adapterChain;
	}

	public synchronized void setNextAdapter(Adapter nextAdapter) {
		this.nextAdapter = nextAdapter;
	}

	public synchronized void setNextAdapterCtx(AdapterContext nextAdapterCtx) {
		this.nextAdapterCtx = nextAdapterCtx;
	}
	
	public synchronized Map<String, Object> getMsg() {
		return msg;
	}

	public synchronized void setMsg(Map<String, Object> msg) {
		this.msg = msg;
	}

	public Object fireNextAdapter(Object data) throws Exception {
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
	
	public Object fireNextException(ExceptionEvent event) throws Exception {
		if (nextAdapter != null) {
			return nextAdapter.onException(nextAdapterCtx, event);
		}
		else {
			// current adapter is the last on the chain
			throw event.getCause();
		}
	}
	
}
