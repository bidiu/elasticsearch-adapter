package cn.com.deepdata.es_adapter.adapter;

import java.util.Map;

import cn.com.deepdata.es_adapter.ExceptionEvent;
import cn.com.deepdata.es_adapter.model.DataWrapper;

/**
 * Adapter's context. 
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
	
	public synchronized Map<String, Object> getMsg() {
		return msg;
	}
	
	public synchronized Class<?> getNextAdapterClazz() {
		return nextAdapter == null ? ReservedTailAdapter.class : nextAdapter.getClass();
	}

	/**
	 * Specific adapters CANNOT call this method.
	 * 
	 * @param dataWrapper
	 * @return
	 * @throws Exception
	 */
	DataWrapper fireNextAdapter(DataWrapper dataWrapper) throws Exception {
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
	
	/**
	 * Specific adapters CANNOT call this method.
	 * 
	 * @param event
	 * @return
	 * @throws Exception
	 */
	DataWrapper fireNextException(ExceptionEvent event) throws Exception {
		if (nextAdapter != null) {
			return nextAdapter.onException(nextAdapterCtx, event);
		}
		else {
			// current adapter is the last on the chain
			throw event.getCause();
		}
	}
	
}
