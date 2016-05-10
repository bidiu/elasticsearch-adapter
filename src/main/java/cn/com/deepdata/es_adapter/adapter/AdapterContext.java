package cn.com.deepdata.es_adapter.adapter;

import java.util.Map;

import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.model.DataWrapper;

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

	public DataWrapper fireNextAdapter(DataWrapper dataWrapper) throws Exception {
		if (nextAdapter != null) {
			if (adapterChain.isInbound()) {
				return nextAdapter.inboundAdapt(nextAdapterCtx, dataWrapper);
			}
			else {
				return nextAdapter.outboundAdapt(nextAdapterCtx, dataWrapper);
			}
		}
		else {
			// current adapter is the last on the chain
			return dataWrapper;
		}
	}
	
	public DataWrapper fireNextException(ExceptionEvent event) throws Exception {
		if (nextAdapter != null) {
			return nextAdapter.onException(nextAdapterCtx, event);
		}
		else {
			// current adapter is the last on the chain
			throw event.getCause();
		}
	}
	
}
